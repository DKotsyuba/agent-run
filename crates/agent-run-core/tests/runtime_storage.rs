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

/// Seals one home carrying a managed Codex plugin version root plus one
/// ordinary skill root, so both switch geometries coexist in one index.
///
/// The plugin tree mirrors the adapter's installed layout — a
/// `.codex-plugin/plugin.json` manifest, a `skills/<name>/SKILL.md` marker
/// whose YAML description carries a unique `ar-<name>-skill-marker` probe,
/// and one payload file — and the home carries the native personal
/// marketplace declaration and `config.toml` install records the real codex
/// binary reads, so a converted home is natively discoverable.
fn seal_plugin(root: &Path, name: &str, payload: &[u8]) -> Sealed {
    let market = root.join(format!("{name}-market"));
    let plugin_source = market.join("plugins").join(name);
    stdfs::create_dir_all(market.join(".agents/plugins")).expect("marketplace dir");
    stdfs::create_dir_all(plugin_source.join(".codex-plugin")).expect("plugin manifest dir");
    stdfs::create_dir_all(plugin_source.join("skills").join(name)).expect("skill dir");
    stdfs::write(
        market.join(".agents/plugins/marketplace.json"),
        format!(
            "{{\"name\":\"personal\",\"interface\":{{\"displayName\":\"agent-run\"}},\
             \"plugins\":[{{\"name\":\"{name}\",\
             \"source\":{{\"source\":\"local\",\"path\":\"./plugins/{name}\"}},\
             \"policy\":{{\"installation\":\"AVAILABLE\",\
             \"authentication\":\"ON_INSTALL\"}}}}]}}\n"
        ),
    )
    .expect("marketplace fixture");
    stdfs::write(
        plugin_source.join(".codex-plugin/plugin.json"),
        format!("{{\"name\":\"{name}\",\"version\":\"1.0.0\",\"description\":\"fixture\",\"skills\":\"./skills\"}}\n"),
    )
    .expect("plugin manifest");
    stdfs::write(
        plugin_source.join("skills").join(name).join("SKILL.md"),
        format!("---\ndescription: ar-{name}-skill-marker\n---\n# {name} skill\n"),
    )
    .expect("skill");
    stdfs::write(plugin_source.join("payload.bin"), payload).expect("payload");
    let skill_source = root.join(format!("{name}-skill-source"));
    stdfs::create_dir_all(&skill_source).expect("skill source");
    stdfs::write(
        skill_source.join("SKILL.md"),
        format!("# {name} plain skill\n"),
    )
    .expect("plain skill");
    let home = root.join(format!("{name}-home"));
    snapshot_tree::snapshot_managed_tree(
        &home,
        &PathBuf::from("plugins/cache/personal")
            .join(name)
            .join("1.0.0"),
        &plugin_source,
        None,
    )
    .expect("plugin tree");
    snapshot_tree::snapshot_managed_tree(&home, Path::new("skills/plain"), &skill_source, None)
        .expect("plain skill tree");
    stdfs::create_dir_all(home.join(".agents/plugins")).expect("marketplace dir");
    stdfs::write(
        home.join(".agents/plugins/marketplace.json"),
        format!(
            "{{\"name\":\"personal\",\"interface\":{{\"displayName\":\"agent-run\"}},\
             \"plugins\":[{{\"name\":\"{name}\",\
             \"source\":{{\"source\":\"local\",\
             \"path\":\"./plugins/cache/personal/{name}/1.0.0\"}},\
             \"policy\":{{\"installation\":\"AVAILABLE\",\
             \"authentication\":\"ON_INSTALL\"}}}}]}}\n"
        ),
    )
    .expect("marketplace declaration");
    stdfs::write(
        home.join("config.toml"),
        format!(
            "key = \"value\"\n\n[marketplaces.personal]\nsource_type = \"local\"\nsource = {:?}\n\n\
             [plugins.\"{name}@personal\"]\nenabled = true\n",
            market
        ),
    )
    .expect("flat config");
    let credential = root.join(format!("{name}-auth.json"));
    stdfs::write(&credential, "{}\n").expect("credential");
    std::os::unix::fs::symlink(&credential, home.join("auth.json")).expect("credential link");
    let revision = format!("revision-{name}");
    let digest = snapshot_tree::finalize_runtime_snapshots(
        &home,
        &revision,
        &[
            "config.toml".into(),
            ".agents/plugins/marketplace.json".into(),
        ],
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

/// One converted plugin home's parent link target, derived like the
/// coordinator derives it.
fn plugin_view_target(
    store: &Path,
    layout: &agent_run_core::state::runtime_storage::RuntimeStorageLayout,
    name: &str,
) -> PathBuf {
    let mapping = layout
        .roots
        .get(&format!("plugins/cache/personal/{name}/1.0.0"))
        .expect("plugin mapping");
    agent_run_platform::plugin_views::view_root(
        store,
        &SharedTreeRef {
            scope: mapping.scope.clone(),
            manifest_sha256: mapping.manifest_sha256.clone(),
        },
        "1.0.0",
    )
    .expect("view target")
}

/// Fails unless every entry at and below `dir` is a real file or directory
/// reached without one symlink — the shape native Codex plugin discovery
/// requires beneath a mounted plugin parent.
fn assert_real_tree(dir: &Path) {
    let metadata = stdfs::symlink_metadata(dir).expect("real entry");
    assert!(!metadata.is_symlink(), "{dir:?} must not be a symlink");
    assert!(metadata.is_dir(), "{dir:?} must be a real directory");
    for entry in stdfs::read_dir(dir).expect("list").flatten() {
        let path = entry.path();
        let metadata = stdfs::symlink_metadata(&path).expect("real entry");
        assert!(!metadata.is_symlink(), "{path:?} must not be a symlink");
        if metadata.is_dir() {
            assert_real_tree(&path);
        }
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

/// A managed Codex plugin root switches by parent mount: the parent is one
/// exact link onto a store container holding the named real version subtree,
/// discovery stays symlink-free, payloads keep one store inode, and the
/// original bytes and strict-verifier refusal are preserved.
#[test]
fn plugin_parent_mount_keeps_real_version_discoverable() {
    let root = TempDir::new().expect("fixture root");
    let sealed = seal_plugin(root.path(), "probe", b"plugin payload\n");
    let app = app(root.path());
    let mut store = Store::initialize(&app.home).expect("store");
    let scope = fs::sha256(b"coordinator-scope");
    let store_root = runtime_storage::store_root(&app.home).unwrap();
    let layout = runtime_storage::plan(
        &store,
        &app.home,
        &sealed.home,
        &sealed.index_sha256,
        &scope,
    )
    .expect("plan")
    .expect("layout");
    assert_eq!(layout.roots.len(), 2);
    runtime_storage::install(&mut store, &app.home, &layout, None).expect("install");

    let parent = sealed.home.join("plugins/cache/personal/probe");
    let target = stdfs::read_link(&parent).expect("parent is one exact link");
    assert_eq!(
        target,
        plugin_view_target(&store_root, &layout, "probe"),
        "the parent link targets the derived readonly view container"
    );
    let version = target.join("1.0.0");
    assert_real_tree(&version);
    assert!(stdfs::symlink_metadata(&version).unwrap().is_dir());
    assert_eq!(
        stdfs::read_to_string(version.join("skills/probe/SKILL.md")).unwrap(),
        "---\ndescription: ar-probe-skill-marker\n---\n# probe skill\n"
    );
    let mapping = layout
        .roots
        .get("plugins/cache/personal/probe/1.0.0")
        .expect("plugin mapping");
    let tree = shared_assets::shared_tree_root(
        &store_root,
        &SharedTreeRef {
            scope: mapping.scope.clone(),
            manifest_sha256: mapping.manifest_sha256.clone(),
        },
    )
    .unwrap();
    assert_eq!(
        stdfs::metadata(version.join("payload.bin")).unwrap().ino(),
        stdfs::metadata(tree.join("payload.bin")).unwrap().ino(),
        "the view payload is the tree's own protected-store inode"
    );
    assert_eq!(
        stdfs::read(version.join(snapshot_tree::SNAPSHOT_MANIFEST)).unwrap(),
        stdfs::read(tree.join(snapshot_tree::SNAPSHOT_MANIFEST)).unwrap(),
        "the original manifest bytes are unchanged"
    );
    // The ordinary skill root keeps the plain whole-tree link geometry.
    assert!(sealed
        .home
        .join("skills/plain")
        .symlink_metadata()
        .expect("plain skill link")
        .file_type()
        .is_symlink());
    runtime_storage::verify(&store, &app.home, &sealed.home, &sealed.index_sha256)
        .expect("bridge verification accepts the parent mount");
    assert_eq!(
        stdfs::read(sealed.home.join(RUNTIME_SNAPSHOT_INDEX)).unwrap(),
        sealed.index_bytes,
        "the frozen index bytes stay byte-exact"
    );
    assert!(
        materialize::verify(&sealed.home, &sealed.index_sha256).is_err(),
        "the strict public verifier still refuses the converted home"
    );
    assert!(backups(&sealed.home).is_empty(), "proven backups removed");
    cleanup_store(&store_root);
}

/// Each crash point in the plugin parent switch leaves a recoverable
/// prepared row that roll-forward recovery completes.
#[test]
fn interrupted_plugin_installs_roll_forward_through_recovery() {
    for point in [
        StorageFault::BeforeRename,
        StorageFault::AfterRename,
        StorageFault::AfterLink,
        StorageFault::AfterCommit,
    ] {
        let root = TempDir::new().expect("fixture root");
        let sealed = seal_plugin(root.path(), "probe", b"plugin payload\n");
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
        runtime_storage::recover(&mut store, &app.home, &sealed.home)
            .expect("roll-forward recovery completes");
        runtime_storage::verify(&store, &app.home, &sealed.home, &sealed.index_sha256)
            .expect("verified after recovery");
        let parent = sealed.home.join("plugins/cache/personal/probe");
        let target = stdfs::read_link(&parent).expect("parent remounted");
        assert_eq!(
            target,
            plugin_view_target(&store_root_for(&app.home), &layout, "probe")
        );
        assert_real_tree(&target.join("1.0.0"));
        assert!(
            backups(&sealed.home).is_empty(),
            "backups cleaned: {point:?}"
        );
        cleanup_store(&store_root_for(&app.home));
    }
}

/// The store root derived from one app home, as a plain path.
fn store_root_for(app_home: &Path) -> PathBuf {
    runtime_storage::store_root(app_home).expect("store root")
}

/// A plugin parent holding anything but its single indexed version — a
/// sibling file or a second indexed version — refuses planning with the
/// original parent preserved untouched.
#[test]
fn plugin_parent_siblings_refuse_planning_without_loss() {
    for (label, second_version) in [("sibling file", false), ("second version", true)] {
        let root = TempDir::new().expect("fixture root");
        let sealed = seal_probe_with_parent_extra(root.path(), "probe", second_version);
        let app = app(root.path());
        let store = Store::initialize(&app.home).expect("store");
        let scope = fs::sha256(b"coordinator-scope");
        let planned = runtime_storage::plan(
            &store,
            &app.home,
            &sealed.home,
            &sealed.index_sha256,
            &scope,
        );
        assert!(planned.is_err(), "{label} must refuse the shared layout");
        let parent = sealed.home.join("plugins/cache/personal/probe");
        assert!(
            parent.symlink_metadata().unwrap().is_dir(),
            "{label}: the original parent stays a real directory"
        );
        assert!(
            parent.join("1.0.0").symlink_metadata().unwrap().is_dir(),
            "{label}: the original version tree stays in place"
        );
        if second_version {
            assert!(
                parent.join("2.0.0").symlink_metadata().unwrap().is_dir(),
                "{label}: the second version is never dropped"
            );
        } else {
            assert!(
                parent.join("remote.json").is_file(),
                "{label}: the sibling metadata is never dropped"
            );
        }
        assert!(
            backups(&sealed.home).is_empty(),
            "{label}: no backup or link was created"
        );
        assert!(
            materialize::verify(&sealed.home, &sealed.index_sha256).is_ok(),
            "{label}: the untouched home still strictly verifies"
        );
    }
}

/// Seals one plugin home whose plugin parent carries an extra entry: either
/// an unindexed sibling file or a second indexed managed version.
fn seal_probe_with_parent_extra(root: &Path, name: &str, second_version: bool) -> Sealed {
    let sealed = seal_plugin(root, name, b"plugin payload\n");
    if second_version {
        let source = root.join(format!("{name}-second-source"));
        stdfs::create_dir_all(source.join("skills").join(name)).expect("second skill dir");
        stdfs::write(
            source.join("skills").join(name).join("SKILL.md"),
            format!("# {name} second skill\n"),
        )
        .expect("second skill");
        snapshot_tree::snapshot_managed_tree(
            &sealed.home,
            &PathBuf::from("plugins/cache/personal")
                .join(name)
                .join("2.0.0"),
            &source,
            None,
        )
        .expect("second version tree");
    } else {
        stdfs::write(
            sealed
                .home
                .join("plugins/cache/personal")
                .join(name)
                .join("remote.json"),
            "{\"metadata\":true}\n",
        )
        .expect("sibling metadata");
    }
    let revision = "revision-probe";
    let digest = snapshot_tree::finalize_runtime_snapshots(
        &sealed.home,
        revision,
        &[
            "config.toml".into(),
            ".agents/plugins/marketplace.json".into(),
        ],
        &[(
            "auth.json".into(),
            root.join(format!("{name}-auth.json"))
                .to_string_lossy()
                .into_owned(),
        )],
    )
    .expect("runtime index");
    let index_bytes = stdfs::read(sealed.home.join(RUNTIME_SNAPSHOT_INDEX)).expect("index");
    Sealed {
        index_sha256: digest,
        index_bytes,
        ..sealed
    }
}

/// A tampered plugin backup and a foreign parent link are refused without
/// losing the originals, and repairing the link restores verification.
#[test]
fn tampered_plugin_backup_and_foreign_parent_link_are_refused() {
    let root = TempDir::new().expect("fixture root");
    let sealed = seal_plugin(root.path(), "probe", b"plugin payload\n");
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
    let backup = sealed
        .home
        .join(&backups(&sealed.home)[0])
        .join("plugins/cache/personal/probe/1.0.0/skills/probe/SKILL.md");
    stdfs::write(&backup, b"tampered\n").expect("tamper staged plugin backup");
    runtime_storage::recover(&mut store, &app.home, &sealed.home)
        .expect_err("an unproven plugin backup is refused");
    assert_eq!(backups(&sealed.home).len(), 1, "the backup stays in place");
    runtime_storage::verify(&store, &app.home, &sealed.home, &sealed.index_sha256)
        .expect("the mounted home itself still verifies");

    let parent = sealed.home.join("plugins/cache/personal/probe");
    let target = stdfs::read_link(&parent).expect("original parent target");
    stdfs::remove_file(&parent).expect("remove parent link");
    std::os::unix::fs::symlink(&app.home, &parent).expect("foreign parent link");
    runtime_storage::verify(&store, &app.home, &sealed.home, &sealed.index_sha256)
        .expect_err("a foreign parent link fails bridge verification");
    stdfs::remove_file(&parent).expect("remove foreign link");
    std::os::unix::fs::symlink(&target, &parent).expect("repair parent link");
    runtime_storage::verify(&store, &app.home, &sealed.home, &sealed.index_sha256)
        .expect("verification returns after repair");
    cleanup_store(&runtime_storage::store_root(&app.home).unwrap());
}

/// Runs one owned child command to completion under a hard 15-second bound,
/// killing it if it exceeds the bound, and returns its captured output.
fn run_bounded(command: &mut std::process::Command) -> std::process::Output {
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
    let mut child = command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn owned child");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("owned child exceeded the 15-second bound");
            }
            Err(error) => panic!("owned child could not be reaped: {error}"),
        }
    }
    child
        .wait_with_output()
        .expect("collect owned child output")
}

/// Native selector: the real codex binary discovers a coordinator-produced
/// parent-mounted plugin — `plugin list --json` reports it installed and the
/// rendered prompt input contains the plugin skill marker — while the plain
/// skill root link and index bytes stay untouched.
///
/// Ignored by default: it shells out to the real `codex` binary (`CODEX_BIN`
/// or `PATH`) against owned fixture homes with no credentials and no model
/// turn. Run it explicitly with
/// `cargo test -p agent-run-core --features test-fixtures --test runtime_storage -- --ignored`.
#[test]
#[ignore = "runs the real codex binary; select it with CODEX_BIN"]
fn native_codex_discovers_coordinator_parent_mount() {
    let codex_command = || {
        std::env::var_os("CODEX_BIN").map_or_else(
            || std::process::Command::new("codex"),
            |binary| std::process::Command::new(PathBuf::from(binary)),
        )
    };
    let root = TempDir::new().expect("fixture root");
    let sealed = seal_plugin(root.path(), "probe", b"plugin payload\n");
    let app = app(root.path());
    let mut store = Store::initialize(&app.home).expect("store");
    let layout = runtime_storage::plan(
        &store,
        &app.home,
        &sealed.home,
        &sealed.index_sha256,
        &fs::sha256(b"coordinator-scope"),
    )
    .expect("plan")
    .expect("layout");
    runtime_storage::install(&mut store, &app.home, &layout, None).expect("install");
    runtime_storage::verify(&store, &app.home, &sealed.home, &sealed.index_sha256)
        .expect("bridge verification before the native probe");

    let home_env = |command: &mut std::process::Command| {
        command
            .env("CODEX_HOME", &sealed.home)
            .env("HOME", root.path())
            .env_remove("CODEX_API_KEY")
            .env_remove("OPENAI_API_KEY");
    };
    let mut list = codex_command();
    home_env(&mut list);
    list.arg("plugin").arg("list").arg("--json");
    let list_output = run_bounded(&mut list);
    assert!(
        list_output.status.success(),
        "codex plugin list failed (marker only, no output dump)"
    );
    let document: serde_json::Value =
        serde_json::from_slice(&list_output.stdout).expect("plugin list JSON");
    let installed = document["installed"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        installed.iter().any(|entry| {
            entry["pluginId"] == "probe@personal" && entry["installed"] == serde_json::json!(true)
        }),
        "native plugin discovery must report the parent-mounted plugin installed"
    );

    let mut prompt = codex_command();
    home_env(&mut prompt);
    prompt.arg("debug").arg("prompt-input").arg("ping");
    let prompt_output = run_bounded(&mut prompt);
    assert!(
        prompt_output.status.success(),
        "codex debug prompt-input failed (marker only, no output dump)"
    );
    let marker = String::from_utf8_lossy(&prompt_output.stdout);
    assert!(
        marker.matches("ar-probe-skill-marker").count() >= 1,
        "the plugin skill marker must stay visible in the rendered prompt input"
    );
    assert!(
        backups(&sealed.home).is_empty(),
        "the native probe leaves no backups"
    );
    cleanup_store(&runtime_storage::store_root(&app.home).unwrap());
}
