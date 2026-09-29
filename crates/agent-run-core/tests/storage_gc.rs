//! Reference-aware collection of the shared managed-asset store.
//! Gated on the crate's deterministic test seams for interrupted installs.
#![cfg(feature = "test-fixtures")]

use agent_run_core::{
    fs,
    runtime_storage::{self, StorageFault},
    state::{runtime_storage::LayoutState, Store},
    storage_gc::{self, Mode},
};
use agent_run_platform::{
    plugin_views, shared_assets::{self, SharedStoreLock, SharedTreeRef}, snapshot_tree,
};
use std::{
    fs as stdfs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};
use tempfile::TempDir;

/// One sealed home fixture with a single managed tree carrying `payload`.
struct Sealed {
    home: PathBuf,
    index_sha256: String,
}

/// Seals one home whose managed tree carries `payload`.
fn seal(root: &Path, name: &str, payload: &[u8]) -> Sealed {
    let source = root.join(format!("{name}-source"));
    stdfs::create_dir_all(source.join("scripts")).expect("source tree");
    stdfs::write(source.join("SKILL.md"), "# skill\n").expect("skill");
    stdfs::write(source.join("scripts/payload.bin"), payload).expect("payload");
    let home = root.join(name);
    snapshot_tree::snapshot_managed_tree(&home, Path::new("skills/demo"), &source, None)
        .expect("managed tree");
    stdfs::write(home.join("config.toml"), "key = \"value\"\n").expect("flat config");
    let digest = snapshot_tree::finalize_runtime_snapshots(
        &home,
        &format!("revision-{name}"),
        &["config.toml".into()],
        &[],
    )
    .expect("runtime index");
    Sealed {
        index_sha256: digest,
        home,
    }
}

/// One canonical app home carrying its own store database.
fn app(root: &Path) -> PathBuf {
    let home = root.join("app");
    stdfs::create_dir_all(&home).expect("app home");
    home.canonicalize().expect("canonical app home")
}

/// Installs one sealed home into the shared store and returns its layout key.
fn share(store: &mut Store, _home_path: &Path, app_home: &Path, sealed: &Sealed) -> String {
    let layout = runtime_storage::plan(
        store,
        app_home,
        &sealed.home,
        &sealed.index_sha256,
        &scope(),
    )
    .expect("plan")
    .expect("managed roots");
    let key = layout.runtime_home.clone();
    runtime_storage::install(store, app_home, &layout, None).expect("install");
    key
}

/// The deterministic shared scope the launch path derives for this user.
fn scope() -> String {
    agent_run_core::supervisor::managed_scope()
}

/// The store root below one app home.
fn store_root(app_home: &Path) -> PathBuf {
    runtime_storage::store_root(app_home).expect("store root")
}

/// Restores owner write permission below one fixture tree so TempDir can drop.
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

/// Collects until a round completes, asserting convergence within a bound.
fn converge(store: &mut Store, app_home: &Path, mode: Mode) -> Vec<storage_gc::Outcome> {
    let mut outcomes = Vec::new();
    for _ in 0..32 {
        let outcome = storage_gc::sweep(store, app_home, mode).expect("collection pass");
        let busy = outcome.backlog();
        outcomes.push(outcome);
        if !busy {
            return outcomes;
        }
    }
    panic!("collection did not converge within a bounded number of passes");
}

/// Names one entry below a store namespace directory.
fn names(store: &Path, namespace: &str) -> Vec<String> {
    let mut found: Vec<String> = Vec::new();
    let Ok(scopes) = stdfs::read_dir(store.join(namespace)) else {
        return found;
    };
    for scope in scopes.flatten() {
        let Ok(entries) = stdfs::read_dir(scope.path()) else {
            continue;
        };
        for entry in entries.flatten() {
            found.push(format!(
                "{}/{}",
                scope.file_name().to_string_lossy(),
                entry.file_name().to_string_lossy()
            ));
        }
    }
    found.sort();
    found
}

/// Two homes sharing one payload keep their objects while both exist, and the
/// last reference with its physical home gone reclaims the objects.
#[test]
fn shared_payload_survives_until_last_reference_is_gone() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let first = seal(root.path(), "one", b"shared-payload\n");
    let second = seal(root.path(), "two", b"shared-payload\n");
    let first_key = share(&mut store, &first.home, &app_home, &first);
    share(&mut store, &second.home, &app_home, &second);
    assert_eq!(first_key, first.home.canonicalize().unwrap().to_string_lossy());
    for outcome in converge(&mut store, &app_home, Mode::Apply) {
        assert_eq!(outcome.trees_removed, 0, "referenced trees are retained");
        assert_eq!(outcome.blobs_removed, 0, "referenced blobs are retained");
        assert_eq!(outcome.rows_removed, 0, "live homes keep their rows");
    }
    // Removing one home leaves the other's objects intact.
    let mut rows_removed = 0;
    let mut trees_removed = 0;
    permit_tree(&first.home);
    stdfs::remove_dir_all(&first.home).expect("first home removed");
    for outcome in converge(&mut store, &app_home, Mode::Apply) {
        rows_removed += outcome.rows_removed;
        trees_removed += outcome.trees_removed;
    }
    assert_eq!(rows_removed, 1, "only the gone home's row goes");
    assert_eq!(trees_removed, 0, "the shared tree stays pinned by the survivor");
    assert!(
        runtime_storage::verify(&store, &app_home, &second.home, &second.index_sha256).is_ok(),
        "the surviving home still verifies through the bridge"
    );
    assert_eq!(names(&store_root(&app_home), "trees").len(), 1);
    // Removing the last home and its row reclaims the tree and its payload.
    permit_tree(&second.home);
    stdfs::remove_dir_all(&second.home).expect("second home removed");
    let outcomes = converge(&mut store, &app_home, Mode::Apply);
    let trees: usize = outcomes.iter().map(|outcome| outcome.trees_removed).sum::<usize>() + trees_removed;
    let blobs: usize = outcomes.iter().map(|outcome| outcome.blobs_removed).sum();
    let rows: usize = outcomes.iter().map(|outcome| outcome.rows_removed).sum::<usize>() + rows_removed;
    assert_eq!(trees, 1, "the last tree is collected: {outcomes:?}");
    assert_eq!(blobs, 2, "both payloads are collected: {outcomes:?}");
    assert_eq!(rows, 2, "both rows are removed once their homes are gone");
    assert!(names(&store_root(&app_home), "trees").is_empty());
    assert!(names(&store_root(&app_home), "blobs").is_empty());
    permit_tree(&store_root(&app_home));
}

/// A prepared row and a committed row whose home still exists both pin their
/// objects; a configuration path or an unreleased service reference into the
/// store pins a tree or a single blob directly.
#[test]
fn prepared_config_and_service_references_pin_objects() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let sealed = seal(root.path(), "home", b"pinned-payload\n");
    let layout = runtime_storage::plan(
        &store,
        &app_home,
        &sealed.home,
        &sealed.index_sha256,
        &scope(),
    )
    .expect("plan")
    .expect("managed roots");
    let manifest = layout.roots["skills/demo"].manifest_sha256.clone();
    runtime_storage::install(&mut store, &app_home, &layout, None).expect("install");

    // A prepared row pins even when its physical home is gone.
    let interrupted = seal(root.path(), "interrupted", b"pinned-payload\n");
    let pending_layout = runtime_storage::plan(
        &store,
        &app_home,
        &interrupted.home,
        &interrupted.index_sha256,
        &scope(),
    )
    .expect("plan")
    .expect("managed roots");
    let fault = |_: StorageFault| -> agent_run_domain::Result<()> {
        Err(agent_run_domain::Error::Validation("simulated crash".into()))
    };
    runtime_storage::install_with_fault(&mut store, &app_home, &pending_layout, None, Some(&fault))
        .expect_err("crash before commit");
    let pending_home = interrupted.home.canonicalize().expect("canonical home");
    for outcome in converge(&mut store, &app_home, Mode::Apply) {
        assert_eq!(outcome.rows_removed, 0, "a prepared row never ages out");
        assert_eq!(outcome.trees_removed, 0, "a prepared row pins its objects");
    }
    let committed = store_root(&app_home);
    assert!(
        names(&committed, "trees")
            .iter()
            .any(|name| name.ends_with(&manifest)),
        "the committed home's tree stays pinned"
    );
    // Recover finishes the prepared row in place; nothing is re-downloaded and
    // both homes keep verifying.
    runtime_storage::recover(&mut store, &app_home, &pending_home).expect("recover");
    let state = store
        .runtime_storage_layout(&pending_home.to_string_lossy())
        .expect("row")
        .expect("present")
        .state;
    assert_eq!(state, LayoutState::Committed);
    runtime_storage::verify(&store, &app_home, &pending_home, &interrupted.index_sha256)
        .expect("recovered home verifies");
    for outcome in converge(&mut store, &app_home, Mode::Apply) {
        assert_eq!(outcome.trees_removed, 0, "both homes still pin their trees");
    }
    // An unreleased service generation whose frozen definition names a shared
    // payload file directly pins that blob even after both homes are gone.
    let blobs = names(&committed, "blobs");
    assert_eq!(blobs.len(), 2, "two distinct payloads stay stored");
    let pinned = blobs
        .iter()
        .find(|name| name.ends_with("-600"))
        .map(|name| name.split('/').next_back().unwrap().to_owned())
        .expect("a plain blob");
    let referenced = committed.join("blobs").join(scope()).join(&pinned);
    let referenced_text = referenced.to_string_lossy().into_owned();
    let definition = format!(
        "{{\"command\":\"{referenced_text}\",\"args\":[],\"cwd\":\"/tmp\",\
         \"readiness\":{{\"command\":\"/bin/true\",\"args\":[],\"deadline_seconds\":1}}}}"
    );
    store
        .conn
        .execute(
            "INSERT INTO managed_service_generations \
             (id,service_id,revision,definition_json,state,broker_identity_json,created_at) \
             VALUES('pin','pin',?1,?2,'starting','{}',?3)",
            [
                "a".repeat(64),
                definition,
                agent_run_core::domain::now().to_string(),
            ],
        )
        .expect("unreleased service generation");
    permit_tree(&sealed.home);
    stdfs::remove_dir_all(&sealed.home).expect("first home gone");
    permit_tree(&interrupted.home);
    stdfs::remove_dir_all(&interrupted.home).expect("second home gone");
    let outcomes = converge(&mut store, &app_home, Mode::Apply);
    assert!(
        referenced.is_file(),
        "the service-referenced blob stays: {referenced:?} {outcomes:?}"
    );
    // Once the generation is stopped and released, nothing references the
    // object any more and a later round reclaims it.
    store
        .conn
        .execute("UPDATE managed_service_generations SET state='stopped'", [])
        .expect("released");
    let outcomes = converge(&mut store, &app_home, Mode::Apply);
    let blobs: usize = outcomes.iter().map(|outcome| outcome.blobs_removed).sum();
    assert!(blobs >= 1, "the unreferenced blob is reclaimed: {outcomes:?}");
    permit_tree(&committed);
}

/// A publisher holding the publish lock defers collection without error, and
/// collection holding the lock defers a publisher: the two serialize.
#[test]
fn publish_and_collection_serialize_through_one_lock() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let sealed = seal(root.path(), "home", b"race-payload\n");
    let key = share(&mut store, &sealed.home, &app_home, &sealed);
    permit_tree(&sealed.home);
    stdfs::remove_dir_all(&sealed.home).expect("home gone");
    let root_store = store_root(&app_home);
    let guard = SharedStoreLock::acquire(&root_store).expect("publisher lock");
    let outcome = storage_gc::sweep(&mut store, &app_home, Mode::Apply).expect("busy pass");
    assert!(outcome.lock_busy, "collection defers to the publisher");
    assert_eq!(outcome.removed(), 0);
    drop(guard);
    let outcomes = converge(&mut store, &app_home, Mode::Apply);
    let rows: usize = outcomes.iter().map(|pass| pass.rows_removed).sum();
    assert_eq!(rows, 1, "the gone home's row is removed once free: {key}");
    let collected = SharedStoreLock::try_acquire(&root_store).expect("collection lock");
    assert!(collected.is_some(), "collection released its lock");
    permit_tree(&root_store);
}

/// Corrupt or unreadable evidence retains the object and reports incomplete.
#[test]
fn corrupt_proof_retains_and_reports_incomplete() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let sealed = seal(root.path(), "home", b"corrupt-payload\n");
    share(&mut store, &sealed.home, &app_home, &sealed);
    permit_tree(&sealed.home);
    stdfs::remove_dir_all(&sealed.home).expect("home gone");
    // Break the row's stored layout so its own digest check fails: the page
    // read then reports an integrity error, which must retain everything.
    store
        .conn
        .execute(
            "UPDATE runtime_storage_layouts SET layout_json='{\"version\":1}'",
            [],
        )
        .expect("corrupt row");
    let mut passes = 0;
    for _ in 0..4 {
        let outcome = storage_gc::sweep(&mut store, &app_home, Mode::Apply).expect("pass");
        passes += 1;
        if !outcome.backlog() {
            break;
        }
    }
    assert!(
        passes == 4,
        "corrupt evidence never becomes a deletion proof"
    );
    assert!(!names(&store_root(&app_home), "trees").is_empty());
    permit_tree(&store_root(&app_home));
}

/// Orphan publisher staging is removed only with the exact owned provenance,
/// and never by age or shape alone.
#[test]
fn staging_orphans_need_exact_provenance() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let sealed = seal(root.path(), "home", b"staging-payload\n");
    share(&mut store, &sealed.home, &app_home, &sealed);
    let root_store = store_root(&app_home);
    let scope = scope();
    let trees_scope = root_store.join("trees").join(&scope);
    let blobs_scope = root_store.join("blobs").join(&scope);
    stdfs::create_dir_all(trees_scope.join(".agent-run-staging-0123456789abcdef0123456789abcdef.tmp"))
        .expect("staging dir");
    stdfs::write(
        blobs_scope.join(".agent-run-staging-0123456789abcdef0123456789abcdef.tmp"),
        b"partial",
    )
    .expect("staging file");
    // A foreign shape and a foreign name stay untouched.
    stdfs::create_dir_all(trees_scope.join("not-staging")).expect("foreign name");
    let mut staging = 0;
    for _ in 0..4 {
        let outcome = storage_gc::sweep(&mut store, &app_home, Mode::Apply).expect("pass");
        staging += outcome.staging_removed;
        if staging == 2 {
            break;
        }
    }
    assert_eq!(staging, 2, "exactly the two owned orphans are removed");
    assert!(trees_scope.join("not-staging").is_dir());
    permit_tree(&root_store);
}

/// A home symlink pointing outside the store is never followed: collection
/// only ever unlinks entries of the store's own namespaces.
#[test]
fn collection_never_follows_symlinks_out_of_the_store() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let sealed = seal(root.path(), "home", b"symlink-payload\n");
    share(&mut store, &sealed.home, &app_home, &sealed);
    let root_store = store_root(&app_home);
    let outside = root.path().join("outside-target");
    stdfs::write(&outside, "precious\n").expect("outside file");
    let scope = scope();
    let trees_scope = root_store.join("trees").join(&scope);
    stdfs::create_dir_all(&trees_scope).expect("scope");
    std::os::unix::fs::symlink(&outside, trees_scope.join("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"))
        .expect("digest-named link");
    let mut retained = false;
    for _ in 0..4 {
        let outcome = storage_gc::sweep(&mut store, &app_home, Mode::Apply).expect("pass");
        retained |= outcome.incomplete;
        assert_eq!(outcome.trees_removed, 0, "an unknown object is never deleted");
    }
    assert!(retained, "a digest-named link reports incomplete evidence");
    assert_eq!(
        stdfs::read_to_string(&outside).expect("outside file intact"),
        "precious\n"
    );
    permit_tree(&root_store);
}

/// The collection helpers agree with the platform store layout: the blob set
/// derived from one manifest names exactly the payloads its tree hardlinks.
#[test]
fn derived_blob_names_match_the_published_layout() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let sealed = seal(root.path(), "home", b"layout-payload\n");
    let layout = runtime_storage::plan(
        &store,
        &app_home,
        &sealed.home,
        &sealed.index_sha256,
        &scope(),
    )
    .expect("plan")
    .expect("managed roots");
    runtime_storage::install(&mut store, &app_home, &layout, None).expect("install");
    let reference = SharedTreeRef {
        scope: scope(),
        manifest_sha256: layout.roots["skills/demo"].manifest_sha256.clone(),
    };
    let blobs = shared_assets::shared_tree_blob_names(&store_root(&app_home), &reference)
        .expect("blob names");
    assert_eq!(blobs.len(), 2, "one skill file and one payload");
    for relative in blobs {
        let path = store_root(&app_home).join(&relative);
        let metadata = stdfs::metadata(&path).expect("blob exists");
        assert_eq!(
            metadata.permissions().mode() & 0o777,
            if relative.to_string_lossy().ends_with("-700") {
                0o500
            } else {
                0o400
            },
            "the physical mode matches the logical suffix"
        );
    }
    permit_tree(&store_root(&app_home));
    let _ = fs::sha256(b"unused");
}

/// Derived plugin-parent views are collected before the trees and payloads
/// beneath them: an obsolete view keeps its payload's inode allocated, and a
/// live view survives exactly while a registered row maps the plugin root.
#[test]
fn views_collect_before_their_trees_and_only_when_unreferenced() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let sealed = seal_plugin(root.path(), "plugins", b"view-payload\n");
    let layout = runtime_storage::plan(
        &store,
        &app_home,
        &sealed.home,
        &sealed.index_sha256,
        &scope(),
    )
    .expect("plan")
    .expect("managed roots");
    runtime_storage::install(&mut store, &app_home, &layout, None).expect("install");
    let store_root = store_root(&app_home);
    let mapping = &layout.roots["plugins/cache/personal/probe/1.0.0"];
    let reference = SharedTreeRef {
        scope: scope(),
        manifest_sha256: mapping.manifest_sha256.clone(),
    };
    // The registered mapping keeps its view alive; a view of a version no row
    // maps is obsolete even while its tree is pinned.
    let live = plugin_views::materialize(&store_root, &reference, "1.0.0")
        .expect("live view");
    let stale = plugin_views::materialize(&store_root, &reference, "2.0.0")
        .expect("stale view");
    assert!(live.is_dir() && stale.is_dir());
    for outcome in converge(&mut store, &app_home, Mode::Apply) {
        assert_eq!(outcome.trees_removed, 0, "the pinned tree stays");
        assert_eq!(outcome.blobs_removed, 0, "the pinned payload stays");
    }
    assert!(live.is_dir(), "a registered mapping keeps its view");
    assert!(!stale.exists(), "an unmapped version's view is collected");

    // Once the home is gone, the view goes first, then the tree and payload.
    permit_tree(&sealed.home);
    stdfs::remove_dir_all(&sealed.home).expect("home gone");
    let outcomes = converge(&mut store, &app_home, Mode::Apply);
    let views: usize = outcomes.iter().map(|pass| pass.views_removed).sum();
    let trees: usize = outcomes.iter().map(|pass| pass.trees_removed).sum();
    let blobs: usize = outcomes.iter().map(|pass| pass.blobs_removed).sum();
    assert_eq!(views, 1, "the live view is collected once unmapped: {outcomes:?}");
    assert_eq!(trees, 1, "the tree follows its view: {outcomes:?}");
    assert_eq!(blobs, 2, "the payloads follow their tree: {outcomes:?}");
    assert!(!live.exists());
    assert!(names(&store_root, "trees").is_empty());
    assert!(names(&store_root, "blobs").is_empty());
    permit_tree(&store_root);
}

/// Seals one home whose managed root is a Codex plugin version directory.
fn seal_plugin(root: &Path, name: &str, payload: &[u8]) -> Sealed {
    let source = root.join(format!("{name}-source"));
    stdfs::create_dir_all(&source).expect("source tree");
    stdfs::write(source.join("plugin.toml"), "name = \"probe\"\n").expect("plugin file");
    stdfs::write(source.join("payload.bin"), payload).expect("payload");
    let home = root.join(name);
    snapshot_tree::snapshot_managed_tree(
        &home,
        Path::new("plugins/cache/personal/probe/1.0.0"),
        &source,
        None,
    )
    .expect("managed tree");
    stdfs::write(home.join("config.toml"), "key = \"value\"\n").expect("flat config");
    let digest = snapshot_tree::finalize_runtime_snapshots(
        &home,
        &format!("revision-{name}"),
        &["config.toml".into()],
        &[],
    )
    .expect("runtime index");
    Sealed {
        index_sha256: digest,
        home,
    }
}
