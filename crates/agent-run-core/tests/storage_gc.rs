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
    plugin_views,
    shared_assets::{self, SharedStoreLock, SharedTreeRef},
    snapshot_tree,
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
    stdfs::write(home.join("config.toml"), "schema_version = 2\n").expect("config");
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

/// Creates the empty shared-store root, so direct imports have a real root.
fn ensure_store(app_home: &Path) {
    fs::private_dir(&store_root(app_home)).expect("store root");
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

/// Returns `true` for a 64 lowercase hexadecimal digit name.
fn is_digest_name(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
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
    assert_eq!(
        first_key,
        first.home.canonicalize().unwrap().to_string_lossy()
    );
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
    assert_eq!(
        trees_removed, 0,
        "the shared tree stays pinned by the survivor"
    );
    assert!(
        runtime_storage::verify(&store, &app_home, &second.home, &second.index_sha256).is_ok(),
        "the surviving home still verifies through the bridge"
    );
    assert_eq!(names(&store_root(&app_home), "trees").len(), 1);
    // Removing the last home and its row reclaims the tree and its payload.
    permit_tree(&second.home);
    stdfs::remove_dir_all(&second.home).expect("second home removed");
    let outcomes = converge(&mut store, &app_home, Mode::Apply);
    let trees: usize = outcomes
        .iter()
        .map(|outcome| outcome.trees_removed)
        .sum::<usize>()
        + trees_removed;
    let blobs: usize = outcomes.iter().map(|outcome| outcome.blobs_removed).sum();
    let rows: usize = outcomes
        .iter()
        .map(|outcome| outcome.rows_removed)
        .sum::<usize>()
        + rows_removed;
    assert_eq!(trees, 1, "the last tree is collected: {outcomes:?}");
    assert_eq!(blobs, 2, "both payloads are collected: {outcomes:?}");
    assert_eq!(rows, 2, "both rows are removed once their homes are gone");
    assert!(names(&store_root(&app_home), "trees").is_empty());
    assert!(names(&store_root(&app_home), "blobs").is_empty());
    permit_tree(&store_root(&app_home));
}

/// A legacy tree published with readonly `0o500` directories is still a
/// verifiable object, and collection still reclaims it: the quarantine
/// rename normalizes the directory to the portable owner-only `0o700`
/// under the store lock before moving it into the staging namespace.
#[test]
fn legacy_readonly_directories_are_verified_and_collected() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let sealed = seal(root.path(), "legacy", b"legacy-payload\n");
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
    let tree = store_root(&app_home)
        .join("trees")
        .join(scope())
        .join(&manifest);
    // Rewind the published directories to the legacy readonly mode.
    for directory in [&tree, &tree.join("scripts")] {
        stdfs::set_permissions(directory, stdfs::Permissions::from_mode(0o500)).unwrap();
    }
    let reference = SharedTreeRef {
        scope: scope(),
        manifest_sha256: manifest.clone(),
    };
    shared_assets::verify_shared_tree(&store_root(&app_home), &reference)
        .expect("legacy directories stay verifiable");
    permit_tree(&sealed.home);
    stdfs::remove_dir_all(&sealed.home).expect("home gone");
    let outcomes = converge(&mut store, &app_home, Mode::Apply);
    assert!(
        outcomes.iter().any(|pass| pass.trees_removed == 1),
        "the legacy tree is collected: {outcomes:?}"
    );
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
        Err(agent_run_domain::Error::Validation(
            "simulated crash".into(),
        ))
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
    assert!(
        blobs >= 1,
        "the unreferenced blob is reclaimed: {outcomes:?}"
    );
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
    let collected = SharedStoreLock::try_acquire(&root_store, true).expect("collection lock");
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
    stdfs::create_dir_all(
        trees_scope.join(".agent-run-staging-0123456789abcdef0123456789abcdef.tmp"),
    )
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
    std::os::unix::fs::symlink(
        &outside,
        trees_scope.join("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
    )
    .expect("digest-named link");
    let mut retained = false;
    for _ in 0..4 {
        let outcome = storage_gc::sweep(&mut store, &app_home, Mode::Apply).expect("pass");
        retained |= outcome.incomplete;
        assert_eq!(
            outcome.trees_removed, 0,
            "an unknown object is never deleted"
        );
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
    let live = plugin_views::materialize(&store_root, &reference, "1.0.0").expect("live view");
    let stale = plugin_views::materialize(&store_root, &reference, "2.0.0").expect("stale view");
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
    assert_eq!(
        views, 1,
        "the live view is collected once unmapped: {outcomes:?}"
    );
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

/// Inserts one valid committed layout row directly, with its own home path,
/// so a test can build a registry larger than one census page.
fn insert_row(store: &Store, home: &Path, manifest: &str) {
    let home = home.to_string_lossy().into_owned();
    let layout = serde_json::json!({
        "version": 1,
        "runtime_home": home,
        "index_sha256": "0".repeat(64),
        "roots": { "skills/demo": { "scope": scope(), "manifest_sha256": manifest } },
    });
    let text = serde_json::to_string(&layout).unwrap();
    let digest = fs::sha256(text.as_bytes());
    store
        .conn
        .execute(
            "INSERT INTO runtime_storage_layouts(runtime_home,index_sha256,layout_json,\
             layout_sha256,state,operation_token,owner_agent_id,updated_at) \
             VALUES(?1,?2,?3,?4,'committed',?5,NULL,0)",
            [
                home,
                "0".repeat(64),
                text,
                digest,
                format!("rt_{}", "a".repeat(30)),
            ],
        )
        .expect("layout row");
}

/// One digest-shaped name.
fn digest(index: usize) -> String {
    format!("{index:064x}")
}

/// A preview changes nothing: no row removal, no lock-file creation, no scan
/// state. A store without a lock file reports `lock_busy` rather than
/// fabricating a synchronized preview.
#[test]
fn preview_is_truly_read_only() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let sealed = seal(root.path(), "gone", b"preview-bytes\n");
    share(&mut store, &sealed.home, &app_home, &sealed);
    permit_tree(&sealed.home);
    stdfs::remove_dir_all(&sealed.home).expect("home gone");
    let root_store = store_root(&app_home);
    // The store has objects but no lock file.
    stdfs::remove_file(root_store.join(".publish.lock")).expect("lock removed");
    assert!(!root_store.join(".publish.lock").exists());
    let before = stdfs::read(app_home.join("state.db")).expect("database");
    let outcome = storage_gc::sweep(&mut store, &app_home, Mode::Preview).expect("preview");
    assert!(
        outcome.lock_busy,
        "no lock file means no synchronized preview"
    );
    assert_eq!(outcome.removed(), 0);
    assert!(
        !root_store.join(".publish.lock").exists(),
        "no lock created"
    );
    assert_eq!(
        stdfs::read(app_home.join("state.db")).expect("database"),
        before,
        "a preview never removes registry rows"
    );
    // With a lock file present, the preview still removes nothing.
    stdfs::write(root_store.join(".publish.lock"), b"").expect("lock");
    let outcome = storage_gc::sweep(&mut store, &app_home, Mode::Preview).expect("preview");
    assert_eq!(outcome.rows_removed, 1, "the gone row is only reported");
    assert_eq!(outcome.trees_removed, 1, "its tree is only reported");
    assert!(
        root_store.join("trees").join(scope()).is_dir(),
        "nothing unlinked"
    );
    assert_eq!(
        stdfs::read(app_home.join("state.db")).expect("database"),
        before,
        "a preview never removes registry rows"
    );
    permit_tree(&root_store);
}

/// A pinned or protected tree whose manifest is corrupt or missing stops
/// destructive blob work; an empty reference set is never fabricated.
#[test]
fn corrupt_or_missing_pinned_manifest_stops_blob_collection() {
    for corrupt in [true, false] {
        let root = TempDir::new().expect("fixture root");
        let app_home = app(root.path());
        let mut store = Store::initialize(&app_home).expect("store");
        let sealed = seal(root.path(), "home", b"pinned-bytes\n");
        share(&mut store, &sealed.home, &app_home, &sealed);
        let root_store = store_root(&app_home);
        let tree = root_store.join("trees").join(scope());
        let manifest_dir: PathBuf = stdfs::read_dir(&tree)
            .expect("trees")
            .flatten()
            .map(|entry| entry.path())
            .next()
            .expect("one tree");
        let manifest = manifest_dir.join(".agent-run-snapshot.json");
        stdfs::set_permissions(&manifest, stdfs::Permissions::from_mode(0o600)).unwrap();
        stdfs::set_permissions(&manifest_dir, stdfs::Permissions::from_mode(0o700)).unwrap();
        if corrupt {
            stdfs::write(&manifest, b"{\"snapshot_version\":1,\"entries\":[]}").expect("corrupt");
        } else {
            stdfs::remove_file(&manifest).expect("missing manifest");
        }
        stdfs::set_permissions(&manifest_dir, stdfs::Permissions::from_mode(0o500)).unwrap();
        let mut pinned_proof_broken = false;
        for _ in 0..3 {
            let outcome = storage_gc::sweep(&mut store, &app_home, Mode::Apply).expect("pass");
            assert_eq!(outcome.blobs_removed, 0, "no blob may be deleted");
            assert_eq!(outcome.trees_removed, 0, "no tree may be deleted");
            pinned_proof_broken |= outcome.incomplete;
            if !outcome.backlog() {
                break;
            }
        }
        assert!(
            pinned_proof_broken,
            "a broken pinned manifest reports incomplete evidence"
        );
        assert_eq!(
            names(&root_store, "blobs").len(),
            2,
            "payloads are retained"
        );
        permit_tree(&root_store);
        permit_tree(&sealed.home);
    }
}

/// A reference census that cannot complete permits no deletion of any kind.
#[test]
fn partial_reference_census_permits_no_deletion() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    // One live home plus one garbage tree, then a registry far beyond the
    // one-page census bound the seam imposes.
    let sealed = seal(root.path(), "live", b"census-bytes\n");
    share(&mut store, &sealed.home, &app_home, &sealed);
    let garbage = seal(root.path(), "garbage", b"census-garbage\n");
    let reference = SharedTreeRef {
        scope: scope(),
        manifest_sha256: {
            let home = garbage.home.join("skills/demo");
            let bytes = stdfs::read(home.join(".agent-run-snapshot.json")).expect("manifest");
            fs::sha256(&bytes)
        },
    };
    let store_root = store_root(&app_home);
    ensure_store(&app_home);
    shared_assets::import_shared_tree(
        &store_root,
        &scope(),
        &garbage.home,
        Path::new("skills/demo"),
    )
    .expect("imported garbage tree");
    permit_tree(&garbage.home);
    stdfs::remove_dir_all(&garbage.home).expect("garbage home gone");
    let rows_before = {
        let (page, _) = store
            .runtime_storage_layouts_page(None, 1000)
            .expect("rows");
        page.len()
    };
    let base = root.path().canonicalize().expect("canonical base");
    for index in 0..300 {
        let home = base.join(format!("row-{index}"));
        stdfs::create_dir_all(&home).expect("row home");
        insert_row(&store, &home, &digest(index));
    }
    std::env::set_var("AGENT_RUN_GC_ROW_PAGES", "1");
    let outcome = storage_gc::sweep(&mut store, &app_home, Mode::Apply).expect("pass");
    std::env::remove_var("AGENT_RUN_GC_ROW_PAGES");
    assert!(outcome.incomplete, "the census is honestly incomplete");
    assert_eq!(outcome.removed(), 0, "no rows, views, trees or blobs go");
    let rows_after = {
        let (page, _) = store
            .runtime_storage_layouts_page(None, 1000)
            .expect("rows");
        page.len()
    };
    assert_eq!(rows_after, rows_before + 300, "every row is retained");
    assert_eq!(
        names(&store_root, "trees").len(),
        2,
        "both trees are retained"
    );
    // Both trees share one skill payload, so three distinct blobs remain.
    assert_eq!(
        names(&store_root, "blobs").len(),
        3,
        "all payloads are retained"
    );
    let _ = reference;
    permit_tree(&store_root);
    permit_tree(&sealed.home);
}

/// A configuration or service path pointing into a view container pins the
/// view, its backing tree and the payloads beneath it.
#[test]
fn protected_view_pins_its_tree_and_payloads() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let sealed = seal_plugin(root.path(), "plugins", b"view-pin\n");
    // Import the tree and materialize a view with no layout row at all: only
    // the service reference keeps them alive.
    let home = sealed.home.join("plugins/cache/personal/probe/1.0.0");
    let manifest = {
        let bytes = stdfs::read(home.join(".agent-run-snapshot.json")).expect("manifest");
        fs::sha256(&bytes)
    };
    let reference = SharedTreeRef {
        scope: scope(),
        manifest_sha256: manifest,
    };
    let store_root = store_root(&app_home);
    ensure_store(&app_home);
    shared_assets::import_shared_tree(
        &store_root,
        &scope(),
        &sealed.home,
        home.strip_prefix(&sealed.home).unwrap(),
    )
    .expect("import");
    let view = plugin_views::materialize(&store_root, &reference, "1.0.0").expect("view");
    let pinned_file = view.join("1.0.0/plugin.toml");
    assert!(pinned_file.is_file());
    let definition = format!(
        "{{\"command\":\"{}\",\"args\":[],\"cwd\":\"/tmp\",\
         \"readiness\":{{\"command\":\"/bin/true\",\"args\":[],\"deadline_seconds\":1}}}}",
        pinned_file.to_string_lossy(),
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
    stdfs::remove_dir_all(&sealed.home).expect("home gone");
    for outcome in converge(&mut store, &app_home, Mode::Apply) {
        assert_eq!(outcome.views_removed, 0, "the protected view stays");
        assert_eq!(outcome.trees_removed, 0, "the backing tree stays");
        assert_eq!(outcome.blobs_removed, 0, "the payloads stay");
    }
    assert!(pinned_file.is_file(), "the referenced file is intact");
    permit_tree(&store_root);
}

/// Hundreds of referenced objects ahead of one garbage object do not starve
/// it, and a publication between passes stays protected.
#[test]
fn many_retained_objects_before_garbage_converge() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let store_root = store_root(&app_home);
    ensure_store(&app_home);
    // Many referenced trees, each named by one unreleased service argument.
    let mut args = Vec::new();
    for index in 0..280 {
        let sealed = seal(
            root.path(),
            &format!("live-{index}"),
            format!("payload-{index}\n").as_bytes(),
        );
        let relative = Path::new("skills/demo");
        let manifest = {
            let bytes = stdfs::read(sealed.home.join(relative).join(".agent-run-snapshot.json"))
                .expect("manifest");
            fs::sha256(&bytes)
        };
        let reference = SharedTreeRef {
            scope: scope(),
            manifest_sha256: manifest,
        };
        shared_assets::import_shared_tree(&store_root, &scope(), &sealed.home, relative)
            .expect("import");
        args.push(
            shared_assets::shared_tree_root(&store_root, &reference)
                .unwrap()
                .join("SKILL.md")
                .to_string_lossy()
                .into_owned(),
        );
        permit_tree(&sealed.home);
        stdfs::remove_dir_all(&sealed.home).expect("live home gone");
    }
    let definition = format!(
        "{{\"command\":\"/bin/true\",\"args\":[{}],\"cwd\":\"/tmp\",\
         \"readiness\":{{\"command\":\"/bin/true\",\"args\":[],\"deadline_seconds\":1}}}}",
        args.iter()
            .map(|arg| format!("\"{arg}\""))
            .collect::<Vec<_>>()
            .join(",")
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
    // One garbage tree after all of them.
    let garbage = seal(root.path(), "garbage", b"garbage-payload\n");
    shared_assets::import_shared_tree(
        &store_root,
        &scope(),
        &garbage.home,
        Path::new("skills/demo"),
    )
    .expect("garbage import");
    permit_tree(&garbage.home);
    stdfs::remove_dir_all(&garbage.home).expect("garbage home gone");
    let outcomes = converge(&mut store, &app_home, Mode::Apply);
    let trees: usize = outcomes.iter().map(|pass| pass.trees_removed).sum();
    let blobs: usize = outcomes.iter().map(|pass| pass.blobs_removed).sum();
    assert_eq!(trees, 1, "the garbage tree behind 300 references goes");
    // The garbage tree's skill payload is shared with the references; only
    // its unique payload goes with it.
    assert_eq!(blobs, 1, "its unique payload goes with it");
    assert_eq!(names(&store_root, "trees").len(), 280, "references stay");
    // A publication between passes is not yet in any memo, and stays safe.
    let late = seal(root.path(), "late", b"late-payload\n");
    let key = share(&mut store, &late.home, &app_home, &late);
    assert!(!key.is_empty());
    for outcome in converge(&mut store, &app_home, Mode::Apply) {
        assert_eq!(outcome.trees_removed, 0, "the late publication stays");
        assert_eq!(outcome.blobs_removed, 0);
    }
    assert!(
        runtime_storage::verify(&store, &app_home, &late.home, &late.index_sha256).is_ok(),
        "the late home still verifies"
    );
    permit_tree(&store_root);
    permit_tree(&late.home);
}

/// A staging directory too large for one drain pass stays resumable, is not
/// counted until it is actually removed, and a foreign file in the blob
/// namespace is never mistaken for a payload.
#[test]
fn staging_partial_drain_resumes_and_foreign_blobs_are_retained() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let root_store = store_root(&app_home);
    let scope_dir = root_store.join("trees").join(scope());
    stdfs::create_dir_all(&scope_dir).expect("scope");
    let staging = scope_dir.join(".agent-run-staging-0123456789abcdef0123456789abcdef.tmp");
    stdfs::create_dir_all(&staging).expect("staging");
    for index in 0..600 {
        stdfs::write(staging.join(format!("f{index:04}")), b"x").expect("staged file");
    }
    let blobs_scope = root_store.join("blobs").join(scope());
    stdfs::create_dir_all(&blobs_scope).expect("blob scope");
    stdfs::write(blobs_scope.join("not-a-blob.txt"), b"foreign").expect("foreign file");
    let mut passes = 0;
    let mut completed = false;
    for _ in 0..8 {
        let outcome = storage_gc::sweep(&mut store, &app_home, Mode::Apply).expect("pass");
        passes += 1;
        if outcome.staging_removed > 0 {
            completed = true;
            break;
        }
        assert!(outcome.incomplete, "an unfinished drain reports backlog");
        assert!(staging.is_dir(), "a partial drain stays in place");
    }
    assert!(completed, "repeated passes finish the drain: {passes}");
    assert!(!staging.exists(), "the finished orphan is removed");
    assert!(
        blobs_scope.join("not-a-blob.txt").is_file(),
        "a foreign file is not a payload and never goes"
    );
    permit_tree(&root_store);
}

/// A home that cannot be resolved conclusively is not gone: its row and
/// objects are retained and the pass reports incomplete evidence.
#[test]
fn unresolvable_home_is_not_gone() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    // A home path whose parent is a regular file resolves with ENOTDIR, which
    // is not a proof of absence.
    let base = root.path().canonicalize().expect("canonical base");
    let blocker = base.join("blocker.txt");
    stdfs::write(&blocker, b"file").expect("blocker");
    let home = blocker.join("runs").join("ag-20260101-000000-0000000001");
    insert_row(&store, &home, &digest(0));
    let root_store = store_root(&app_home);
    ensure_store(&app_home);
    let sealed = seal(root.path(), "garbage", b"unresolvable\n");
    shared_assets::import_shared_tree(
        &root_store,
        &scope(),
        &sealed.home,
        Path::new("skills/demo"),
    )
    .expect("import");
    permit_tree(&sealed.home);
    stdfs::remove_dir_all(&sealed.home).expect("home gone");
    let outcome = storage_gc::sweep(&mut store, &app_home, Mode::Apply).expect("pass");
    assert!(
        outcome.rows_removed == 0,
        "an unresolvable home keeps its row: {outcome:?}"
    );
    assert!(outcome.incomplete, "the uncertainty is reported");
    permit_tree(&root_store);
    permit_tree(&sealed.home);
}

/// An unreadable or invalid current configuration is uncertainty, not proof
/// of no references: a would-be-collectable object stays.
#[test]
fn invalid_configuration_retains_everything() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let garbage = seal(root.path(), "garbage", b"invalid-config\n");
    let store_root = store_root(&app_home);
    ensure_store(&app_home);
    shared_assets::import_shared_tree(
        &store_root,
        &scope(),
        &garbage.home,
        Path::new("skills/demo"),
    )
    .expect("garbage import");
    permit_tree(&garbage.home);
    stdfs::remove_dir_all(&garbage.home).expect("garbage home gone");
    stdfs::write(app_home.join("config.toml"), "not = valid config\n{{{\n").expect("broken");
    let outcome = storage_gc::sweep(&mut store, &app_home, Mode::Apply).expect("pass");
    assert!(outcome.incomplete, "a broken configuration is uncertainty");
    assert_eq!(outcome.removed(), 0, "nothing may be deleted");
    assert_eq!(
        names(&store_root, "trees").len(),
        1,
        "the object is retained"
    );
    permit_tree(&store_root);
}

/// A garbage tree far larger than one drain pass is moved into the staging
/// namespace before any byte is unlinked, so a partially drained object never
/// remains a canonical published tree, and repeated passes converge.
#[test]
fn oversized_garbage_tree_moves_to_staging_and_converges() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let source = root.path().join("big-source");
    stdfs::create_dir_all(&source).expect("source");
    // Sized just under the manifest's own metadata bound: the safety property
    // under test is the atomic move before any unlink, and a staging drain
    // larger than one unlink budget is covered by the staging test below.
    for index in 0..460 {
        stdfs::write(source.join(format!("f{index:04}.bin")), b"x").expect("payload");
    }
    let home = root.path().join("big");
    snapshot_tree::snapshot_managed_tree(&home, Path::new("skills/big"), &source, None)
        .expect("managed tree");
    stdfs::write(home.join("config.toml"), "k = \"v\"\n").expect("config");
    let digest =
        snapshot_tree::finalize_runtime_snapshots(&home, "rev-big", &["config.toml".into()], &[])
            .expect("index");
    let layout = runtime_storage::plan(&store, &app_home, &home, &digest, &scope())
        .expect("plan")
        .expect("managed roots");
    runtime_storage::install(&mut store, &app_home, &layout, None).expect("install");
    let store_root = store_root(&app_home);
    permit_tree(&home);
    stdfs::remove_dir_all(&home).expect("home gone");
    // First pass removes the row and moves the tree; later passes drain it.
    let mut moved = false;
    let mut drained = false;
    for _ in 0..16 {
        let outcome = storage_gc::sweep(&mut store, &app_home, Mode::Apply).expect("pass");
        if outcome.trees_removed > 0 {
            moved = true;
            // The invariant: once removed, the object is never a canonical
            // published tree again — anything still draining lives under the
            // recognized staging name, which no verifier ever has to prove.
            let scope_dir = store_root.join("trees").join(scope());
            let canonical: Vec<_> = stdfs::read_dir(&scope_dir)
                .expect("scope")
                .flatten()
                .filter(|entry| is_digest_name(&entry.file_name().to_string_lossy()))
                .collect();
            assert!(
                canonical.is_empty(),
                "a removed tree never remains under its canonical name"
            );
            for staged in stdfs::read_dir(&scope_dir).expect("scope").flatten() {
                assert!(
                    staged
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".agent-run-staging-"),
                    "only staging names drain: {:?}",
                    staged.file_name()
                );
            }
        }
        if moved && names(&store_root, "trees").is_empty() {
            drained = true;
            break;
        }
    }
    assert!(moved, "the tree was moved out of its canonical name");
    assert!(drained, "repeated passes finish the drain");
    assert!(names(&store_root, "blobs").is_empty(), "payloads follow");
    permit_tree(&store_root);
}

/// A configuration or service reference to a whole scope container — not a
/// leaf — protects every tree, view and payload beneath it.
#[test]
fn ancestor_scope_reference_pins_every_object_beneath_it() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let store_root = store_root(&app_home);
    ensure_store(&app_home);
    for name in ["one", "two"] {
        let sealed = seal(root.path(), name, format!("scope-{name}\n").as_bytes());
        shared_assets::import_shared_tree(
            &store_root,
            &scope(),
            &sealed.home,
            Path::new("skills/demo"),
        )
        .expect("import");
        permit_tree(&sealed.home);
        stdfs::remove_dir_all(&sealed.home).expect("home gone");
    }
    // One view, referenced only through the scope container.
    let sealed = seal_plugin(root.path(), "plugins", b"scope-view\n");
    let home = sealed.home.join("plugins/cache/personal/probe/1.0.0");
    let manifest = {
        let bytes = stdfs::read(home.join(".agent-run-snapshot.json")).expect("manifest");
        fs::sha256(&bytes)
    };
    let reference = SharedTreeRef {
        scope: scope(),
        manifest_sha256: manifest,
    };
    shared_assets::import_shared_tree(
        &store_root,
        &scope(),
        &sealed.home,
        home.strip_prefix(&sealed.home).unwrap(),
    )
    .expect("import");
    let view = plugin_views::materialize(&store_root, &reference, "1.0.0").expect("view");
    permit_tree(&sealed.home);
    stdfs::remove_dir_all(&sealed.home).expect("home gone");
    // The service names the scope containers, never a leaf.
    let definition = format!(
        "{{\"command\":\"/bin/true\",\"args\":[\"{}\",\"{}\"],\"cwd\":\"/tmp\",\
         \"readiness\":{{\"command\":\"/bin/true\",\"args\":[],\"deadline_seconds\":1}}}}",
        store_root.join("trees").join(scope()).to_string_lossy(),
        store_root
            .join(plugin_views::VIEW_NAMESPACE)
            .join(scope())
            .to_string_lossy(),
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
    for outcome in converge(&mut store, &app_home, Mode::Apply) {
        assert_eq!(outcome.trees_removed, 0, "an ancestor pins every tree");
        assert_eq!(outcome.views_removed, 0, "an ancestor pins every view");
        assert_eq!(outcome.blobs_removed, 0, "an ancestor pins every payload");
    }
    assert_eq!(names(&store_root, "trees").len(), 3, "all trees stay");
    assert!(view.is_dir(), "the view stays");
    permit_tree(&store_root);
}

/// A reference to a view scope container, namespace root, or store ancestor
/// protects each view in it and the tree and payloads beneath it, with no
/// tree or blob reference of their own in the census.
#[test]
fn view_scope_and_namespace_references_pin_backing_trees() {
    for label in ["scope", "namespace", "store"] {
        let root = TempDir::new().expect("fixture root");
        let app_home = app(root.path());
        let mut store = Store::initialize(&app_home).expect("store");
        let store_root = store_root(&app_home);
        ensure_store(&app_home);
        let sealed = seal_plugin(root.path(), "plugins", b"view-scope\n");
        let home = sealed.home.join("plugins/cache/personal/probe/1.0.0");
        let manifest = {
            let bytes = stdfs::read(home.join(".agent-run-snapshot.json")).expect("manifest");
            fs::sha256(&bytes)
        };
        let reference = SharedTreeRef {
            scope: scope(),
            manifest_sha256: manifest,
        };
        shared_assets::import_shared_tree(
            &store_root,
            &scope(),
            &sealed.home,
            home.strip_prefix(&sealed.home).unwrap(),
        )
        .expect("import");
        let view = plugin_views::materialize(&store_root, &reference, "1.0.0").expect("view");
        permit_tree(&sealed.home);
        stdfs::remove_dir_all(&sealed.home).expect("home gone");
        // The only reference names a view container or its ancestor,
        // never a separate tree, blob or leaf path.
        let namespace = store_root.join(plugin_views::VIEW_NAMESPACE);
        let pinned = match label {
            "scope" => namespace.join(scope()),
            "namespace" => namespace,
            _ => store_root.clone(),
        }
        .to_string_lossy()
        .into_owned();
        let definition = format!(
            "{{\"command\":\"/bin/true\",\"args\":[\"{pinned}\"],\"cwd\":\"/tmp\",\
             \"readiness\":{{\"command\":\"/bin/true\",\"args\":[],\"deadline_seconds\":1}}}}"
        );
        store
            .conn
            .execute(
                "INSERT INTO managed_service_generations \
                 (id,service_id,revision,definition_json,state,broker_identity_json,created_at) \
                 VALUES(?1,?1,?2,?3,'starting','{}',?4)",
                [
                    format!("pin-{label}"),
                    "a".repeat(64),
                    definition,
                    agent_run_core::domain::now().to_string(),
                ],
            )
            .expect("unreleased service generation");
        for outcome in converge(&mut store, &app_home, Mode::Apply) {
            assert_eq!(outcome.views_removed, 0, "{label}: the view stays");
            assert_eq!(outcome.trees_removed, 0, "{label}: the backing tree stays");
            assert_eq!(outcome.blobs_removed, 0, "{label}: the payloads stay");
        }
        assert!(view.is_dir(), "{label}: the view is intact");
        assert_eq!(
            names(&store_root, "trees").len(),
            1,
            "{label}: the backing tree is intact"
        );
        assert!(
            !names(&store_root, "blobs").is_empty(),
            "{label}: the payloads are intact"
        );
        permit_tree(&store_root);
    }
}

/// A home whose existence cannot be determined — here a parent directory the
/// effective user cannot search — keeps its row and reports incomplete
/// evidence; only a plain `NotFound` on both resolutions proves absence.
#[test]
fn unreadable_home_parent_keeps_row_and_reports_incomplete() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let base = root.path().canonicalize().expect("canonical base");
    let secret = base.join("secret");
    stdfs::create_dir_all(&secret).expect("secret parent");
    let home = secret.join("runs").join("ag-20260101-000000-0000000001");
    stdfs::create_dir_all(&home).expect("home");
    insert_row(&store, &home, &digest(0));
    // Garbage the pass would otherwise collect.
    let garbage = seal(root.path(), "garbage", b"unreadable\n");
    let store_root = store_root(&app_home);
    ensure_store(&app_home);
    shared_assets::import_shared_tree(
        &store_root,
        &scope(),
        &garbage.home,
        Path::new("skills/demo"),
    )
    .expect("import");
    permit_tree(&garbage.home);
    stdfs::remove_dir_all(&garbage.home).expect("garbage home gone");
    stdfs::set_permissions(&secret, stdfs::Permissions::from_mode(0o000)).expect("sealed");
    let outcome = storage_gc::sweep(&mut store, &app_home, Mode::Apply).expect("pass");
    stdfs::set_permissions(&secret, stdfs::Permissions::from_mode(0o700)).expect("unsealed");
    assert_eq!(
        outcome.rows_removed, 0,
        "an unreadable home keeps its row: {outcome:?}"
    );
    assert!(
        outcome.incomplete,
        "unknown existence is reported, not guessed: {outcome:?}"
    );
    permit_tree(&store_root);
}

/// A live home whose own directory is unreadable keeps its row, its tree,
/// its payloads and the native object it links: an unreadable live home is
/// uncertainty, never an unreferenced one, so the pass reports incomplete
/// evidence and deletes nothing.
#[test]
fn unreadable_home_itself_keeps_row_and_all_objects() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let sealed = seal(root.path(), "sealed", b"unreadable-home\n");
    share(&mut store, &sealed.home, &app_home, &sealed);
    let root_store = store_root(&app_home);
    // One native cache object the home references through one exact link.
    let bytes = b"native-object\n".to_vec();
    let object = root_store
        .join("native-cache")
        .join(scope())
        .join(fs::sha256(&bytes));
    stdfs::create_dir_all(object.parent().expect("native scope")).expect("native scope");
    stdfs::write(&object, &bytes).expect("native object");
    stdfs::set_permissions(&object, stdfs::Permissions::from_mode(0o400)).expect("object mode");
    let cache = sealed
        .home
        .join(agent_run_core::native_cache::TOOLS_CACHE_DIR);
    stdfs::create_dir_all(&cache).expect("home cache dir");
    std::os::unix::fs::symlink(&object, cache.join(format!("{:040x}.json", 7)))
        .expect("exact native link");
    // The home itself — not its parent — becomes unreadable.
    stdfs::set_permissions(&sealed.home, stdfs::Permissions::from_mode(0o000)).expect("sealed");
    let outcome = storage_gc::sweep(&mut store, &app_home, Mode::Apply).expect("pass");
    stdfs::set_permissions(&sealed.home, stdfs::Permissions::from_mode(0o700)).expect("restored");
    assert!(
        outcome.incomplete,
        "an unreadable live home is reported: {outcome:?}"
    );
    assert_eq!(outcome.removed(), 0, "nothing may be deleted: {outcome:?}");
    assert_eq!(
        outcome.rows_removed, 0,
        "a live home keeps its row: {outcome:?}"
    );
    assert!(object.is_file(), "the linked native object is retained");
    assert_eq!(
        names(&root_store, "trees").len(),
        1,
        "the home's tree is retained"
    );
    assert_eq!(
        names(&root_store, "blobs").len(),
        2,
        "the home's payloads are retained"
    );
    permit_tree(&root_store);
}

/// A prepared row whose physical home is missing still pins what it names:
/// the row is kept and the imported tree and payloads stay. The pass itself
/// completes — an absent home holds no native references — so retention is
/// proven by the pin, not by an aborted pass.
#[test]
fn prepared_row_pins_even_when_physical_home_is_missing() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let mut store = Store::initialize(&app_home).expect("store");
    let interrupted = seal(root.path(), "interrupted", b"prepared-pin\n");
    let layout = runtime_storage::plan(
        &store,
        &app_home,
        &interrupted.home,
        &interrupted.index_sha256,
        &scope(),
    )
    .expect("plan")
    .expect("managed roots");
    let manifest = layout.roots["skills/demo"].manifest_sha256.clone();
    let fault = |_: StorageFault| -> agent_run_domain::Result<()> {
        Err(agent_run_domain::Error::Validation(
            "simulated crash".into(),
        ))
    };
    runtime_storage::install_with_fault(&mut store, &app_home, &layout, None, Some(&fault))
        .expect_err("crash before commit");
    let pending_home = interrupted.home.canonicalize().expect("canonical home");
    let state = store
        .runtime_storage_layout(&pending_home.to_string_lossy())
        .expect("row")
        .expect("present")
        .state;
    assert_eq!(state, LayoutState::Prepared);
    permit_tree(&pending_home);
    stdfs::remove_dir_all(&pending_home).expect("physical home gone");
    let root_store = store_root(&app_home);
    let outcome = storage_gc::sweep(&mut store, &app_home, Mode::Apply).expect("pass");
    assert!(
        !outcome.incomplete,
        "the pin, not an aborted pass, retains: {outcome:?}"
    );
    assert_eq!(
        outcome.removed(),
        0,
        "a prepared row pins everything: {outcome:?}"
    );
    assert_eq!(
        outcome.rows_removed, 0,
        "the prepared row is retained: {outcome:?}"
    );
    assert!(
        names(&root_store, "trees")
            .iter()
            .any(|name| name.ends_with(&manifest)),
        "the pinned tree stays"
    );
    assert_eq!(
        names(&root_store, "blobs").len(),
        2,
        "the pinned payloads stay"
    );
    permit_tree(&root_store);
}

/// A staging orphan a dead process left half drained is finished by a later
/// pass on a freshly opened store; the object was already renamed out of its
/// canonical name before the first byte was unlinked.
#[test]
fn partial_staging_orphan_drains_after_restart() {
    let root = TempDir::new().expect("fixture root");
    let app_home = app(root.path());
    let store = Store::initialize(&app_home).expect("store");
    let store_root = store_root(&app_home);
    let scope_dir = store_root.join("trees").join(scope());
    stdfs::create_dir_all(&scope_dir).expect("scope");
    // The state an interrupted drain leaves: a staging name, already partly
    // unlinked, and no canonical tree anywhere.
    let staging = scope_dir.join(".agent-run-staging-fedcba9876543210fedcba9876543210.tmp");
    stdfs::create_dir_all(&staging).expect("staging");
    for index in 0..600 {
        stdfs::write(staging.join(format!("f{index:04}")), b"x").expect("staged file");
    }
    for index in 0..300 {
        stdfs::remove_file(staging.join(format!("f{index:04}"))).expect("half drained");
    }
    assert!(!scope_dir.join("0".repeat(64)).exists());
    // A restarted process opens its own store handle and finds exactly this.
    drop(store);
    let mut store = Store::open(&app_home).expect("reopened store");
    let mut removed = 0;
    let mut passes = 0;
    for _ in 0..8 {
        let outcome = storage_gc::sweep(&mut store, &app_home, Mode::Apply).expect("pass");
        passes += 1;
        removed += outcome.staging_removed;
        if !staging.exists() {
            break;
        }
        assert!(
            outcome.incomplete,
            "an unfinished drain reports backlog: {outcome:?}"
        );
    }
    assert!(!staging.exists(), "repeated passes finish the orphan");
    assert_eq!(
        removed, 1,
        "only the completed removal is counted: {passes}"
    );
    permit_tree(&store_root);
}
