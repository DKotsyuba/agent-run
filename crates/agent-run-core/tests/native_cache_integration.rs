//! End-to-end native cache integration on disposable fixtures: anchor,
//! freeze, guard detection, packed metadata caches, protected GC, thaw before
//! native work, and collection once the last physical home is gone.
//!
//! No provider process is started and no model turn runs: the lifecycle glue
//! is driven directly with a synthetic frozen identity whose authority binds
//! the fixture home's real sealed index, exactly what the supervisor holds
//! after sealing.
#![cfg(feature = "test-fixtures")]

mod common;

use agent_run_core::{
    native_cache::{self},
    native_tree_cache, runtime_cache, runtime_storage,
    state::Store,
    storage_gc::{self, Mode},
};
use agent_run_domain::catalog::HarnessId;
use std::{
    collections::BTreeMap,
    fs as stdfs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
};
use tempfile::TempDir;

/// One sealed cache-only home: a finalized index with no managed roots, the
/// generated system skills tree, one remote plugin parent with its native
/// marker, and both metadata caches.
struct Fixture {
    _root: TempDir,
    app_home: PathBuf,
    home: PathBuf,
    identity: agent_run_core::service::ProviderLaunchIdentity,
    account: agent_run_domain::AccountId,
    #[allow(dead_code)]
    payload: Vec<u8>,
}

/// The native remote-plugin marker a real native install leaves behind.
fn remote_marker(plugin: &str) -> String {
    format!("{{\"schema_version\":1,\"remote_plugin_id\":\"{plugin}\"}}\n")
}

/// One native tools-cache document at the real disk schema.
fn tools_document(entries: usize) -> Vec<u8> {
    let mut body = String::from("{\"schema_version\":4,\"tools\":[");
    for index in 0..entries {
        if index > 0 {
            body.push(',');
        }
        body.push_str(&format!("{{\"name\":\"tool-{index}\",\"version\":\"1\"}}"));
    }
    body.push_str("]}\n");
    body.into_bytes()
}

/// One native server-info document at the real disk schema.
fn server_info_document() -> Vec<u8> {
    b"{\"schema_version\":1,\"server\":{\"name\":\"fixture\"}}\n".to_vec()
}

/// Builds the app home, the sealed cache-only home and the frozen identity.
fn fixture(account: &str, payload: &[u8]) -> Fixture {
    let root = TempDir::new().expect("fixture root");
    let app_home = root.path().join("app");
    stdfs::create_dir_all(&app_home).expect("app home");
    stdfs::write(app_home.join("config.toml"), "schema_version = 2\n").expect("config");
    let app_home = app_home.canonicalize().expect("canonical app home");

    let home = root.path().join("codex-home");
    stdfs::create_dir_all(&home).expect("home");
    // The native units require a canonical home path, exactly as the sealed
    // launch path records one.
    let home = home.canonicalize().expect("canonical home");
    // The generated system skills tree.
    let system = home.join("skills/.system");
    stdfs::create_dir_all(system.join("nested")).expect("skills tree");
    stdfs::write(system.join("SKILL.md"), "# system skill\n").expect("skill");
    stdfs::write(system.join("nested/payload.bin"), payload).expect("payload");
    // One remote plugin parent carrying the native install marker.
    let parent = home.join("plugins/cache/remote/fixture-plugin");
    stdfs::create_dir_all(parent.join("1.0.0")).expect("plugin parent");
    stdfs::write(parent.join("1.0.0/plugin.toml"), "name = \"fixture\"\n").expect("plugin file");
    stdfs::write(
        parent.join(".codex-remote-plugin-install.json"),
        remote_marker("fixture-plugin"),
    )
    .expect("native marker");
    // Both metadata caches under their real native identity filenames.
    for (dir, bytes) in [
        ("cache/codex_apps_tools", tools_document(3)),
        ("cache/codex_apps_server_info", server_info_document()),
    ] {
        let cache = home.join(dir);
        stdfs::create_dir_all(&cache).expect("cache dir");
        stdfs::write(cache.join(format!("{}.json", "a".repeat(40))), bytes).expect("cache entry");
    }
    stdfs::write(home.join("config.toml"), "role = \"explore\"\n").expect("flat config");
    let digest = agent_run_platform::snapshot_tree::finalize_runtime_snapshots(
        &home,
        "revision-native",
        &["config.toml".into()],
        &[],
    )
    .expect("sealed index");

    let config_text = format!(
        "schema_version = 2\n\
         [harnesses.codex]\n\
         binary = \"/usr/bin/true\"\n\
         home = \"{codex_home}\"\n\
         [harnesses.claude-code]\n\
         binary = \"/usr/bin/true\"\n\
         home = \"{claude_home}\"\n\
         [providers.codex-user]\n\
         harness = \"codex\"\n\
         connection = {{ kind = \"native\" }}\n\
         auth_family = \"openai\"\n\
         limits_source = \"none\"\n\
         [[providers.codex-user.models]]\n\
         id = \"fixture\"\n\
         native_model = \"fixture\"\n\
         [[providers.codex-user.bindings]]\n\
         label = \"work\"\n\
         account = \"{account}\"\n",
        codex_home = root.path().join("codex").display(),
        claude_home = root.path().join("claude").display(),
        account = account,
    );
    let config = agent_run_config::provider_config::ProviderConfig::parse(&config_text, &app_home)
        .expect("provider config");
    let authority = agent_run_domain::catalog::ResolvedLaunchAuthority {
        provider: "codex-user".parse().expect("provider id"),
        harness: HarnessId::Codex,
        connection: agent_run_domain::ProviderConnection::Native,
        model: "fixture".into(),
        effort: None,
        profile: "review".into(),
        workdir: app_home.clone(),
        role_payload: common::role_payload(),
        assets_sha256: digest.parse().expect("asset digest"),
        eligible_accounts: vec![account.parse().expect("account id")],
    };
    let identity = agent_run_core::service::ProviderLaunchIdentity {
        provider_identity_version: 2,
        replay_request_sha256: "0".repeat(64),
        provider_request: serde_json::from_value(serde_json::json!({
            "provider":"codex-user","model":"fixture","profile":"explore",
            "task":"fixture task","workdir":app_home,"account":account,
        }))
        .expect("request"),
        provider_config_sha256: config.snapshot().expect("snapshot")["sha256"]
            .as_str()
            .expect("digest text")
            .to_owned(),
        provider_config: config,
        provider_config_snapshot: serde_json::json!({}),
        authority,
        runtime_home: Some(home.clone()),
        snapshot_sha256: Some(digest.as_str().into()),
    };
    Fixture {
        _root: root,
        app_home,
        home,
        identity,
        account: account.parse().expect("account id"),
        payload: payload.to_vec(),
    }
}

/// Obtains the validated store root through the real publication-root body.
fn witness(app_home: &Path) -> PathBuf {
    agent_run_core::supervisor::shared_publication_root(app_home, None).expect("validated")
}

/// Restores owner write below one fixture tree so the temporary dir can drop.
fn permit(path: &Path) {
    if let Ok(metadata) = stdfs::symlink_metadata(path)
        && metadata.is_dir()
    {
        let _ = stdfs::set_permissions(path, stdfs::Permissions::from_mode(0o700));
        if let Ok(children) = stdfs::read_dir(path) {
            for child in children.flatten() {
                permit(&child.path());
            }
        }
    }
}

/// Names one entry below a store namespace directory.
fn names(store: &Path, namespace: &str) -> Vec<String> {
    let mut found = Vec::new();
    let Ok(scopes) = stdfs::read_dir(store.join(namespace)) else {
        return found;
    };
    for scope in scopes.flatten() {
        if let Ok(entries) = stdfs::read_dir(scope.path()) {
            for entry in entries.flatten() {
                found.push(format!(
                    "{}/{}",
                    scope.file_name().to_string_lossy(),
                    entry.file_name().to_string_lossy()
                ));
            }
        }
    }
    found.sort();
    found
}

/// Measures unique inode bytes below one path, never following a symlink.
fn unique_bytes(path: &Path) -> u64 {
    fn walk(dir: &Path, seen: &mut Vec<u64>, total: &mut u64) {
        let Ok(entries) = stdfs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let child = entry.path();
            let Ok(metadata) = stdfs::symlink_metadata(&child) else {
                continue;
            };
            if metadata.is_dir() {
                walk(&child, seen, total);
            } else if metadata.is_file() && !seen.contains(&metadata.ino()) {
                seen.push(metadata.ino());
                *total += metadata.len();
            }
        }
    }
    let (mut seen, mut total) = (Vec::new(), 0);
    walk(path, &mut seen, &mut total);
    total
}

/// Counts every store object across the native namespaces.
fn all_objects(store: &Path) -> (usize, usize, usize) {
    (
        names(store, "trees").len(),
        names(store, "blobs").len(),
        names(store, native_cache::NATIVE_CACHE_NAMESPACE).len(),
    )
}

/// Collects until a pass reports no backlog.
fn converge(store: &mut Store, app_home: &Path) -> Vec<storage_gc::Outcome> {
    let mut outcomes = Vec::new();
    for _ in 0..16 {
        let outcome = storage_gc::sweep(store, app_home, Mode::Apply).expect("pass");
        let busy = outcome.backlog();
        outcomes.push(outcome);
        if !busy {
            return outcomes;
        }
    }
    panic!("collection did not converge");
}

/// The full lifecycle on one cache-only home: anchor, freeze, pack, GC
/// retention while the home exists, and collection once the last physical
/// home is gone. Measured unique inode bytes never claim physical savings.
#[test]
fn cache_only_home_anchor_freeze_pack_collect_lifecycle() {
    let fixture = fixture("acct-one", b"native-payload\n");
    let mut store = Store::initialize(&fixture.app_home).expect("store");
    let report = runtime_cache::consolidate(
        &mut store,
        &"ag-20260101-000000-0000000001".parse().unwrap(),
        &fixture.identity,
        &fixture.account,
        &fixture.app_home,
        &fixture.home,
        &witness(&fixture.app_home),
    )
    .expect("consolidation");
    assert_eq!(
        report.frozen, 2,
        "skills root and remote parent: {report:?}"
    );
    assert!(report.packed >= 2, "both metadata caches: {report:?}");
    assert_eq!(
        report.retained, 0,
        "every eligible entry shared: {report:?}"
    );

    // The anchor: a committed row with an empty managed map, keyed by the
    // canonical home, keeping the home in the collector's census.
    let key = fixture
        .home
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let row = store
        .runtime_storage_layout(&key)
        .expect("row")
        .expect("anchored");
    assert_eq!(
        row.state,
        agent_run_store::runtime_storage::LayoutState::Committed
    );
    assert!(
        row.layout().roots.is_empty(),
        "cache-only anchor maps no root"
    );
    // The home still verifies through the coordinator with an empty map.
    runtime_storage::verify(
        &store,
        &fixture.app_home,
        &fixture.home,
        fixture.identity.authority.assets_sha256.as_str(),
    )
    .expect("bridge verify with an empty map");
    // Idempotent: a second consolidation changes nothing structural.
    let again = runtime_cache::consolidate(
        &mut store,
        &"ag-20260101-000000-0000000001".parse().unwrap(),
        &fixture.identity,
        &fixture.account,
        &fixture.app_home,
        &fixture.home,
        &witness(&fixture.app_home),
    )
    .expect("second consolidation");
    assert_eq!(again.already_frozen, 2, "roots re-verify: {again:?}");
    assert_eq!(again.frozen, 0);
    assert!(again.already_shared >= 2);

    let root = runtime_storage::store_root(&fixture.app_home).unwrap();
    let (trees, blobs, native) = all_objects(&root);
    assert!(
        trees >= 2 && blobs >= 3 && native >= 2,
        "{trees} {blobs} {native}"
    );
    // Measured logical sharing on this disposable fixture, reported as unique
    // inode bytes only: never a physical APFS savings claim.
    eprintln!(
        "fixture bytes: shared-store unique={}, home private after sharing={}",
        unique_bytes(&root),
        unique_bytes(&fixture.home)
    );
    // GC retains everything while the anchored home exists.
    for outcome in converge(&mut store, &fixture.app_home) {
        assert_eq!(outcome.trees_removed, 0, "anchored home pins its trees");
        assert_eq!(outcome.blobs_removed, 0, "and their payloads");
        assert_eq!(outcome.native_removed, 0, "and its metadata objects");
        assert_eq!(outcome.rows_removed, 0, "and its registry row");
    }
    assert_eq!(all_objects(&root), (trees, blobs, native));

    eprintln!(
        "fixture bytes idle-after-collection: store unique={}",
        unique_bytes(&root)
    );
    // Once the physical home is gone, the row and every object follow.
    permit(&fixture.home);
    stdfs::remove_dir_all(&fixture.home).expect("home gone");
    let outcomes = converge(&mut store, &fixture.app_home);
    let removed: usize = outcomes.iter().map(|pass| pass.rows_removed).sum();
    assert_eq!(removed, 1, "the anchor row goes");
    assert_eq!(all_objects(&root), (0, 0, 0), "every object is collected");
    permit(&root);
}

/// Two independent homes under one compatible domain share assets, keep
/// distinct private state, and a different account never converges on them.
#[test]
fn two_homes_share_assets_but_keep_private_state_and_domains_isolate() {
    let first = fixture("acct-one", b"shared-native-payload\n");
    let mut store = Store::initialize(&first.app_home).expect("store");
    runtime_cache::consolidate(
        &mut store,
        &"ag-20260101-000000-0000000001".parse().unwrap(),
        &first.identity,
        &first.account,
        &first.app_home,
        &first.home,
        &witness(&first.app_home),
    )
    .expect("first consolidation");
    let root = runtime_storage::store_root(&first.app_home).unwrap();
    let (trees, blobs, native) = all_objects(&root);
    assert!(trees >= 2 && native >= 2);

    // A second home with identical cache content converges on the same
    // objects: one payload under one scope is exactly one object.
    let second = fixture("acct-one", b"shared-native-payload\n");
    // The second fixture lives in its own temporary root; move its home
    // beside the first store's app home so one store serves both.
    let second_home = first.app_home.parent().unwrap().join("second-home");
    permit(&second.home);
    stdfs::rename(&second.home, &second_home).expect("moved home");
    let mut second_identity = second.identity.clone();
    second_identity.runtime_home = Some(second_home.clone());
    let report = runtime_cache::consolidate(
        &mut store,
        &"ag-20260101-000000-0000000002".parse().unwrap(),
        &second_identity,
        &second.account,
        &first.app_home,
        &second_home,
        &witness(&first.app_home),
    )
    .expect("second consolidation");
    // Each home switches its own roots; identical content converges on the
    // very same objects rather than publishing anything new.
    assert_eq!(
        report.frozen, 2,
        "each home switches its own roots: {report:?}"
    );
    assert_eq!(all_objects(&root), (trees, blobs, native), "no new objects");
    // Private state stays private and distinct.
    assert_eq!(
        stdfs::read_to_string(second_home.join("config.toml")).unwrap(),
        stdfs::read_to_string(first.home.join("config.toml")).unwrap(),
        "each home keeps its own flat config"
    );
    assert!(first.home.join("skills/.system").is_symlink());
    assert!(second_home.join("skills/.system").is_symlink());
    let first_link = stdfs::read_link(first.home.join("skills/.system")).unwrap();
    let second_link = stdfs::read_link(second_home.join("skills/.system")).unwrap();
    assert_eq!(
        first_link, second_link,
        "identical content, identical target"
    );

    // A different account identity never converges on the same objects.
    let third = fixture("acct-two", b"shared-native-payload\n");
    let third_home = first.app_home.parent().unwrap().join("third-home");
    permit(&third.home);
    stdfs::rename(&third.home, &third_home).expect("moved home");
    let mut third_identity = third.identity.clone();
    third_identity.runtime_home = Some(third_home.clone());
    let report = runtime_cache::consolidate(
        &mut store,
        &"ag-20260101-000000-0000000003".parse().unwrap(),
        &third_identity,
        &third.account,
        &first.app_home,
        &third_home,
        &witness(&first.app_home),
    )
    .expect("third consolidation");
    assert_eq!(
        report.frozen, 2,
        "a foreign domain publishes afresh: {report:?}"
    );
    assert!(all_objects(&root).0 > trees, "its trees never converge");
    permit(&root);
    permit(&second_home);
    permit(&third_home);
    permit(&first.home);
}

/// Before native work, frozen remote plugin parents thaw while the generated
/// skills tree keeps its link, and a cache-only home still reports shared
/// links so the launch path keeps it bound to the shared store.
#[test]
fn prepare_native_thaws_remote_parents_and_detects_shared_links() {
    let fixture = fixture("acct-one", b"thaw-payload\n");
    let mut store = Store::initialize(&fixture.app_home).expect("store");
    runtime_cache::consolidate(
        &mut store,
        &"ag-20260101-000000-0000000001".parse().unwrap(),
        &fixture.identity,
        &fixture.account,
        &fixture.app_home,
        &fixture.home,
        &witness(&fixture.app_home),
    )
    .expect("consolidation");
    let root = runtime_storage::store_root(&fixture.app_home).unwrap();
    assert!(
        fixture
            .home
            .join("plugins/cache/remote/fixture-plugin")
            .is_symlink()
    );
    assert!(
        runtime_cache::holds_shared_links(&fixture.app_home, &fixture.home).unwrap(),
        "a cache-only home still reads through shared links"
    );

    let thawed =
        runtime_cache::prepare_native(&fixture.app_home, &fixture.home, true).expect("prepare");
    assert_eq!(thawed, 1, "exactly the remote parent thaws");
    let parent = fixture.home.join("plugins/cache/remote/fixture-plugin");
    assert!(!parent.is_symlink(), "the parent is private again");
    assert!(
        parent.join("1.0.0/plugin.toml").is_file(),
        "its content survived"
    );
    assert!(
        fixture.home.join("skills/.system").is_symlink(),
        "the generated skills tree keeps its link"
    );
    // The second call is a no-op: nothing frozen remains to thaw.
    assert_eq!(
        runtime_cache::prepare_native(&fixture.app_home, &fixture.home, true).unwrap(),
        0
    );
    permit(&root);
    permit(&fixture.home);
}

/// A native refresh privatizes one metadata entry; the next consolidation
/// re-shares it, and a pending native journal recovers on the next entry.
#[test]
fn refresh_privatizes_and_repacks_and_journals_recover() {
    let fixture = fixture("acct-one", b"journal-payload\n");
    let mut store = Store::initialize(&fixture.app_home).expect("store");
    let id: agent_run_core::domain::AgentId = "ag-20260101-000000-0000000001".parse().unwrap();
    runtime_cache::consolidate(
        &mut store,
        &id,
        &fixture.identity,
        &fixture.account,
        &fixture.app_home,
        &fixture.home,
        &witness(&fixture.app_home),
    )
    .expect("consolidation");
    let root = runtime_storage::store_root(&fixture.app_home).unwrap();
    let entry = fixture
        .home
        .join(native_cache::TOOLS_CACHE_DIR)
        .join(format!("{}.json", "a".repeat(40)));
    assert!(entry.is_symlink(), "the entry is shared");

    // Native refresh replaces the link with a private file, exactly as the
    // native rename does; the shared object stays untouched.
    stdfs::remove_file(&entry).expect("link removed");
    stdfs::write(&entry, tools_document(4)).expect("refreshed");
    assert!(!entry.is_symlink());
    let report = runtime_cache::consolidate(
        &mut store,
        &id,
        &fixture.identity,
        &fixture.account,
        &fixture.app_home,
        &fixture.home,
        &witness(&fixture.app_home),
    )
    .expect("repack");
    assert!(
        report.packed >= 1,
        "the refreshed entry re-shares: {report:?}"
    );
    assert!(entry.is_symlink());

    // An interrupted operation journal recovers from its own record.
    native_tree_cache::freeze(
        &root,
        &fixture.home,
        "skills/.system",
        &runtime_cache::native_domain(&fixture.identity, &fixture.account).unwrap(),
    )
    .expect("second freeze of a frozen root verifies instead");
    native_tree_cache::recover(&root, &fixture.home).expect("recover");
    permit(&root);
    permit(&fixture.home);
}

/// A private or cache-only home verifies through the shared bridge with an
/// empty map exactly as strictly as a private home, which binding every
/// launch to the store requires.
#[test]
fn private_home_launches_bound_to_the_shared_store() {
    let fixture = fixture("acct-one", b"bound-launch\n");
    // The store exists because another home shared its caches.
    witness(&fixture.app_home);
    let root = runtime_storage::store_root(&fixture.app_home).unwrap();
    assert!(root.is_dir());

    // The empty-map binding verifies the home exactly as strictly as private.
    let assets = agent_run_adapters::provider::SharedLaunchAssets {
        store_root: root.clone(),
        roots: BTreeMap::new(),
    };
    agent_run_adapters::materialize::verify_with_shared(
        &fixture.home,
        fixture.identity.authority.assets_sha256.as_str(),
        &root,
        &assets.roots,
    )
    .expect("a private home verifies through the empty shared binding");

    permit(&root);
    permit(&fixture.home);
}

/// A genuinely new execution's absent home is not an error, while a home a
/// frozen identity already recorded must exist: only the latter refuses.
#[test]
fn absent_home_distinguishes_new_from_retained() {
    let fixture = fixture("acct-one", b"absent\n");
    let witness_root = runtime_storage::store_root(&fixture.app_home).unwrap();
    stdfs::create_dir_all(&witness_root).expect("store exists");
    let absent = fixture
        .app_home
        .join("runs")
        .join("ag-20260101-000000-9999999999");
    // New execution: nothing sealed yet, home not created yet.
    assert_eq!(
        runtime_cache::prepare_native(&fixture.app_home, &absent, false).unwrap(),
        0,
        "a new home has nothing to recover"
    );
    // Retained: a frozen identity recorded this home; its absence is an error.
    let error = runtime_cache::prepare_native(&fixture.app_home, &absent, true).unwrap_err();
    assert!(
        error.to_string().contains("sealed runtime home is missing"),
        "{error}"
    );
    permit(&witness_root);
}

/// Ordinary private names this system never froze never block a launch, while
/// an unreadable or foreign ancestor is an explicit error, never a silent
/// skip that could thaw nothing.
#[test]
fn unsupported_names_skip_but_uncertainty_errors() {
    let fixture = fixture("acct-one", b"names\n");
    let mut store = Store::initialize(&fixture.app_home).expect("store");
    runtime_cache::consolidate(
        &mut store,
        &"ag-20260101-000000-0000000001".parse().unwrap(),
        &fixture.identity,
        &fixture.account,
        &fixture.app_home,
        &fixture.home,
        &witness(&fixture.app_home),
    )
    .expect("consolidation");

    // An ordinary private directory with a space in its name, beside the
    // frozen parent: never a thaw candidate, never a launch failure.
    let odd = fixture.home.join("plugins/cache/downloads/my cache dir");
    stdfs::create_dir_all(&odd).expect("ordinary name");
    // Ordinary metadata files at the cache and market levels are never
    // parents and never block a launch either.
    stdfs::write(fixture.home.join("plugins/cache/.DS_Store"), b"meta").expect("cache file");
    stdfs::write(fixture.home.join("plugins/cache/remote/.DS_Store"), b"meta")
        .expect("market file");
    assert_eq!(
        runtime_cache::prepare_native(&fixture.app_home, &fixture.home, true).unwrap(),
        1,
        "only the frozen remote parent thaws"
    );
    assert!(odd.is_dir(), "the ordinary name is untouched");

    // An unreadable market directory is uncertainty: propagate, do not skip.
    let market = fixture.home.join("plugins/cache/remote");
    stdfs::set_permissions(&market, stdfs::Permissions::from_mode(0o000)).expect("sealed");
    let error = runtime_cache::prepare_native(&fixture.app_home, &fixture.home, true).unwrap_err();
    stdfs::set_permissions(&market, stdfs::Permissions::from_mode(0o755)).expect("unsealed");
    assert!(
        error.to_string().contains("denied") || error.to_string().contains("Permission"),
        "the unreadable ancestor propagates: {error}"
    );

    // A foreign symlink in place of a market is never silently skipped.
    let outside = fixture.app_home.join("outside");
    stdfs::create_dir_all(&outside).expect("outside");
    let symlinked = fixture.home.join("plugins/cache/linked");
    std::os::unix::fs::symlink(&outside, &symlinked).expect("foreign market");
    let error = runtime_cache::prepare_native(&fixture.app_home, &fixture.home, true).unwrap_err();
    assert!(
        error.to_string().contains("directory") || error.to_string().contains("link"),
        "a foreign ancestor propagates: {error}"
    );
    permit(&runtime_storage::store_root(&fixture.app_home).unwrap());
    permit(&fixture.home);
}

/// The curated clone joins the same lifecycle: consolidation freezes its
/// working tree and Git packs beside the other native roots, native
/// preparation thaws both before any launch while private Git state keeps
/// its identity, a later consolidation refreezes onto the same trees, GC
/// retains them while the home exists, and collects them once the last
/// physical home is gone.
#[test]
fn curated_clone_lifecycle_thaws_before_launch_and_collects_after_last_home() {
    let fixture = fixture("acct-one", b"curated-lifecycle\n");
    let private = common::curated_clone(&fixture.home, &common::curated_shape_small(8192), "one");
    let before = common::private_identity(&private);
    let mut store = Store::initialize(&fixture.app_home).expect("store");
    let id: agent_run_core::domain::AgentId = "ag-20260101-000000-0000000001".parse().unwrap();
    let consolidate = |store: &mut Store| {
        runtime_cache::consolidate(
            store,
            &id,
            &fixture.identity,
            &fixture.account,
            &fixture.app_home,
            &fixture.home,
            &witness(&fixture.app_home),
        )
        .expect("consolidation")
    };
    let report = consolidate(&mut store);
    assert_eq!(
        report.frozen, 4,
        "skills, curated tree and packs, remote parent: {report:?}"
    );
    let curated = [
        native_tree_cache::CURATED_MIRROR_ROOT,
        native_tree_cache::CURATED_PACK_ROOT,
    ];
    let targets: Vec<PathBuf> = curated
        .iter()
        .map(|root_key| stdfs::read_link(fixture.home.join(root_key)).expect("curated link"))
        .collect();
    assert_eq!(
        common::private_identity(&private),
        before,
        "freeze keeps Git state"
    );

    let thawed =
        runtime_cache::prepare_native(&fixture.app_home, &fixture.home, true).expect("prepare");
    assert_eq!(thawed, 3, "both curated roots and the remote parent thaw");
    for root_key in curated {
        assert!(
            fixture
                .home
                .join(root_key)
                .symlink_metadata()
                .unwrap()
                .is_dir()
        );
    }
    assert_eq!(
        common::private_identity(&private),
        before,
        "thaw keeps Git state"
    );

    let again = consolidate(&mut store);
    assert_eq!(again.frozen, 3, "thawed roots refreeze: {again:?}");
    for (root_key, target) in curated.iter().zip(&targets) {
        assert_eq!(
            &stdfs::read_link(fixture.home.join(root_key)).unwrap(),
            target
        );
    }
    let root = runtime_storage::store_root(&fixture.app_home).unwrap();
    let objects = all_objects(&root);
    for outcome in converge(&mut store, &fixture.app_home) {
        assert_eq!(
            outcome.trees_removed + outcome.blobs_removed,
            0,
            "the home pins"
        );
    }
    assert_eq!(all_objects(&root), objects);
    permit(&fixture.home);
    stdfs::remove_dir_all(&fixture.home).expect("home gone");
    converge(&mut store, &fixture.app_home);
    assert_eq!(
        all_objects(&root),
        (0, 0, 0),
        "every curated object is collected"
    );
    permit(&root);
}
