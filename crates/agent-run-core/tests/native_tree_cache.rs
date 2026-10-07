//! Native directory-cache lifecycle checks: freeze, thaw, recovery, census.
//! Gated on the crate's deterministic test-seams feature.
#![cfg(feature = "test-fixtures")]

mod common;

use agent_run_core::native_tree_cache::{self, FreezeOutcome, NativeCacheKind, NativeRefScan};
use agent_run_platform::{
    fs,
    shared_assets::{self, SharedTreeRef},
    snapshot_tree::{self, RUNTIME_SNAPSHOT_INDEX, SNAPSHOT_MANIFEST},
};
use std::{
    fs as stdfs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};
use tempfile::TempDir;

/// Valid schema-1 remote-plugin install marker bytes.
const MARKER: &str = "{\"schema_version\":1,\"remote_plugin_id\":\"rp-4f2a\"}\n";

/// Writes one real system-skills tree into `home`.
fn system_skills(home: &Path, files: &[(&str, &[u8])]) {
    for (name, payload) in files {
        let target = home.join("skills/.system").join(name);
        stdfs::create_dir_all(target.parent().expect("parent")).expect("skill dir");
        stdfs::write(&target, payload).expect("skill file");
    }
}

/// Writes one remote plugin parent into `home`; `marker` `None` leaves the
/// parent without a remote-install marker, `Some` writes those exact bytes.
fn remote_parent(home: &Path, payload: &[u8], marker: Option<&str>) {
    let parent = home.join("plugins/cache/fixture/probe");
    let version = parent.join("1.0.0");
    stdfs::create_dir_all(version.join(".codex-plugin")).expect("plugin manifest dir");
    stdfs::create_dir_all(version.join("skills/probe")).expect("version dir");
    stdfs::write(
        version.join(".codex-plugin/plugin.json"),
        "{\"name\":\"probe\",\"version\":\"1.0.0\"}\n",
    )
    .expect("plugin manifest");
    stdfs::write(version.join("skills/probe/SKILL.md"), payload).expect("skill");
    stdfs::write(version.join("payload.bin"), payload).expect("payload");
    if let Some(marker) = marker {
        stdfs::write(parent.join(".codex-remote-plugin-install.json"), marker)
            .expect("remote marker");
    }
}

/// Creates one canonical store root and returns it.
fn store(root: &Path) -> PathBuf {
    let path = root.join("app/shared-assets/v1");
    stdfs::create_dir_all(&path).expect("store root");
    path.canonicalize().expect("canonical store")
}

/// One retained home carrying both native caches and an untouched history
/// file; the optional managed index is finalized when `indexed` is set.
fn home(root: &Path, name: &str, payload: &[u8], marker: Option<&str>, indexed: bool) -> PathBuf {
    let home = root.join(name);
    stdfs::create_dir_all(&home).expect("home");
    system_skills(&home, &[("skill.md", payload), ("nested/deep.md", payload)]);
    remote_parent(&home, payload, marker);
    stdfs::write(home.join("history.json"), b"{\"sessions\":[]}\n").expect("history");
    if indexed {
        let source = root.join(format!("{name}-managed"));
        stdfs::create_dir_all(&source).expect("managed source");
        stdfs::write(source.join("SKILL.md"), "# managed\n").expect("managed skill");
        snapshot_tree::snapshot_managed_tree(&home, Path::new("skills/demo"), &source, None)
            .expect("managed tree");
        stdfs::write(home.join("config.toml"), "key = \"value\"\n").expect("config");
        snapshot_tree::finalize_runtime_snapshots(
            &home,
            &format!("revision-{name}"),
            &["config.toml".into()],
            &[],
        )
        .expect("index");
    }
    home
}

/// One trusted compatibility scope value.
fn scope() -> String {
    fs::sha256(b"native-cache-domain-a")
}

/// Returns the reference one home's controlled root link names.
fn linked_reference(store_root: &Path, home: &Path, root_key: &str) -> SharedTreeRef {
    let target = stdfs::read_link(home.join(root_key)).expect("controlled link");
    let suffix = target
        .strip_prefix(store_root.join("trees"))
        .expect("store target");
    let mut parts = suffix.components();
    SharedTreeRef {
        scope: parts
            .next()
            .and_then(|part| part.as_os_str().to_str())
            .expect("scope")
            .to_owned(),
        manifest_sha256: parts
            .next()
            .and_then(|part| part.as_os_str().to_str())
            .expect("digest")
            .to_owned(),
    }
}

/// Sums regular-file bytes once per unique inode below `paths`, never
/// following symlinks; this is logical reuse evidence, not APFS free space.
fn unique_inode_bytes(paths: &[PathBuf]) -> u64 {
    let mut seen = std::collections::BTreeSet::new();
    let mut total = 0_u64;
    let mut stack: Vec<PathBuf> = paths.to_vec();
    while let Some(path) = stack.pop() {
        let Ok(metadata) = stdfs::symlink_metadata(&path) else {
            continue;
        };
        if metadata.is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            if let Ok(entries) = stdfs::read_dir(&path) {
                for entry in entries.flatten() {
                    stack.push(entry.path());
                }
            }
        } else {
            let identity = (metadata.dev(), metadata.ino());
            if seen.insert(identity) {
                total += metadata.len();
            }
        }
    }
    total
}

/// Makes every readonly store directory removable, then drops a fixture.
fn cleanup_store(store_root: &Path) {
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
    permit(store_root);
}

/// Classification accepts exactly the two native cache shapes.
#[test]
fn classify_accepts_only_native_cache_shapes() {
    assert_eq!(
        native_tree_cache::classify("skills/.system").unwrap(),
        NativeCacheKind::SystemSkills
    );
    assert_eq!(
        native_tree_cache::classify("plugins/cache/fixture/probe").unwrap(),
        NativeCacheKind::RemotePluginParent
    );
    assert_eq!(
        native_tree_cache::classify(".tmp/plugins/plugins").unwrap(),
        NativeCacheKind::CuratedMirror
    );
    assert_eq!(
        native_tree_cache::classify(".tmp/plugins/.git/objects/pack").unwrap(),
        NativeCacheKind::CuratedPacks
    );
    for refused in [
        ".tmp",
        ".tmp/plugins",
        ".tmp/plugins/.git",
        ".tmp/plugins/.git/objects",
        ".tmp/plugins/.git/objects/pack/pack-1.pack",
        ".tmp/plugins/.git/refs",
        ".tmp/plugins/.agents",
        ".tmp/plugins/plugins/plugin-00",
        ".tmp/plugins.sha",
        ".tmp/plugins.sync.lock",
        "plugins/cache/personal/probe",
        "plugins/data/probe",
        "plugins/cache/fixture",
        "plugins/cache/fixture/probe/1.0.0",
        "skills/custom",
        "plugins/cache/fix ture/probe",
    ] {
        assert!(native_tree_cache::classify(refused).is_err(), "{refused}");
    }
}

/// Two idle homes converge on one tree target and payload inode, and the
/// measured unique-inode bytes drop from private duplicates to one shared
/// copy across the original, mid-freeze, and idle-after points.
#[test]
fn two_idle_homes_converge_on_one_shared_tree() {
    let root = TempDir::new().expect("fixture root");
    let payload = vec![7_u8; 64 * 1024];
    let store_root = store(root.path());
    let first = home(root.path(), "first", &payload, Some(MARKER), false);
    let second = home(root.path(), "second", &payload, Some(MARKER), false);
    let original = unique_inode_bytes(&[first.clone(), second.clone()]);

    let outcome =
        native_tree_cache::freeze(&store_root, &first, "plugins/cache/fixture/probe", &scope())
            .expect("first freeze");
    let reference = match outcome {
        FreezeOutcome::Frozen(reference) => reference,
        other => panic!("unexpected outcome: {other:?}"),
    };
    let mid = unique_inode_bytes(&[first.clone(), second.clone(), store_root.clone()]);
    let again = native_tree_cache::freeze(
        &store_root,
        &second,
        "plugins/cache/fixture/probe",
        &scope(),
    )
    .expect("second freeze");
    let second_reference = match again {
        FreezeOutcome::Frozen(reference) => reference,
        other => panic!("unexpected outcome: {other:?}"),
    };
    assert_eq!(reference, second_reference, "identical trees converge");
    let idle = unique_inode_bytes(&[first.clone(), second.clone(), store_root.clone()]);

    let tree = shared_assets::shared_tree_root(&store_root, &reference).unwrap();
    let inode = stdfs::metadata(tree.join("1.0.0/payload.bin"))
        .unwrap()
        .ino();
    for source in [&first, &second] {
        let link = source.join("plugins/cache/fixture/probe");
        assert!(link.symlink_metadata().unwrap().is_symlink());
        assert_eq!(
            stdfs::metadata(link.join("1.0.0/payload.bin"))
                .unwrap()
                .ino(),
            inode,
            "each home resolves the one shared payload inode"
        );
    }
    assert!(
        original > mid && mid > idle,
        "duplication drains monotonically: {original} -> {mid} -> {idle}"
    );
    assert!(
        original - idle >= payload.len() as u64,
        "at least one private payload copy was removed: {original} -> {idle}"
    );
    cleanup_store(&store_root);
}

/// Thaw restores independent writable regular files and leaves the other
/// home's shared bytes untouched.
#[test]
fn thaw_restores_independent_writable_files() {
    let root = TempDir::new().expect("fixture root");
    let payload = b"native payload bytes\n".repeat(512);
    let store_root = store(root.path());
    let first = home(root.path(), "first", &payload, Some(MARKER), false);
    let second = home(root.path(), "second", &payload, Some(MARKER), false);
    for source in [&first, &second] {
        native_tree_cache::freeze(&store_root, source, "plugins/cache/fixture/probe", &scope())
            .expect("freeze");
    }
    let reference = linked_reference(&store_root, &first, "plugins/cache/fixture/probe");
    let tree = shared_assets::shared_tree_root(&store_root, &reference).unwrap();
    let store_inode = stdfs::metadata(tree.join("1.0.0/payload.bin"))
        .unwrap()
        .ino();
    let other_inode = stdfs::metadata(second.join("plugins/cache/fixture/probe/1.0.0/payload.bin"))
        .unwrap()
        .ino();

    let thawed = native_tree_cache::thaw(&store_root, &first, "plugins/cache/fixture/probe")
        .expect("thaw")
        .expect("a thaw happened");
    assert_eq!(thawed, reference);
    let restored = first.join("plugins/cache/fixture/probe");
    assert!(restored.symlink_metadata().unwrap().is_dir());
    assert!(!restored.is_symlink());
    let restored_inode = stdfs::metadata(restored.join("1.0.0/payload.bin"))
        .unwrap()
        .ino();
    assert_ne!(restored_inode, store_inode, "no store hardlink leaks out");
    assert_eq!(
        stdfs::read(restored.join("1.0.0/skills/probe/SKILL.md")).unwrap(),
        payload
    );
    // The restored tree is writable and mutations stay private.
    stdfs::write(restored.join("1.0.0/payload.bin"), b"mutated\n").expect("writable");
    assert_eq!(
        stdfs::read(tree.join("1.0.0/payload.bin")).unwrap(),
        payload,
        "the store copy is unchanged"
    );
    assert_eq!(
        stdfs::metadata(second.join("plugins/cache/fixture/probe/1.0.0/payload.bin"))
            .unwrap()
            .ino(),
        other_inode,
        "the other home's bytes are unchanged"
    );
    // No snapshot manifest leaks into the restored native tree.
    assert!(!restored.join("1.0.0").join(SNAPSHOT_MANIFEST).exists());
    // A second thaw of the now-private root is an explicit no-op.
    assert!(
        native_tree_cache::thaw(&store_root, &first, "plugins/cache/fixture/probe")
            .expect("idempotent thaw")
            .is_none()
    );
    cleanup_store(&store_root);
}

/// A native version update written into a thawed parent freezes into a new
/// tree while the old shared tree stays byte-identical in the store.
#[test]
fn native_update_after_thaw_freezes_a_new_tree() {
    let root = TempDir::new().expect("fixture root");
    let payload = b"version one bytes\n".repeat(256);
    let store_root = store(root.path());
    let home_path = home(root.path(), "home", &payload, Some(MARKER), false);
    let root_key = "plugins/cache/fixture/probe";
    native_tree_cache::freeze(&store_root, &home_path, root_key, &scope()).expect("freeze");
    let first = linked_reference(&store_root, &home_path, root_key);
    native_tree_cache::thaw(&store_root, &home_path, root_key)
        .expect("thaw")
        .expect("thawed");

    // The native SDK writes a new version into the real parent.
    let new_version = home_path.join(root_key).join("2.0.0");
    stdfs::create_dir_all(new_version.join("skills/probe")).expect("new version dir");
    stdfs::write(new_version.join("skills/probe/SKILL.md"), b"version two\n").expect("new skill");
    stdfs::write(
        home_path
            .join(root_key)
            .join(".codex-remote-plugin-install.json"),
        "{\"schema_version\":1,\"remote_plugin_id\":\"rp-4f2a\",\"version\":\"2.0.0\"}\n",
    )
    .expect("updated marker");

    let outcome =
        native_tree_cache::freeze(&store_root, &home_path, root_key, &scope()).expect("refreeze");
    let second = match outcome {
        FreezeOutcome::Frozen(reference) => reference,
        other => panic!("unexpected outcome: {other:?}"),
    };
    assert_ne!(first.manifest_sha256, second.manifest_sha256);
    let old_tree = shared_assets::shared_tree_root(&store_root, &first).unwrap();
    shared_assets::verify_shared_tree(&store_root, &first).expect("old tree retained");
    assert_eq!(
        stdfs::read(old_tree.join("1.0.0/payload.bin")).unwrap(),
        payload,
        "the old shared tree is unchanged"
    );
    let target = stdfs::read_link(home_path.join(root_key)).expect("new link");
    assert_eq!(
        target,
        shared_assets::shared_tree_root(&store_root, &second).unwrap()
    );
    cleanup_store(&store_root);
}

/// The system-skills root keeps its link and bytes on an idempotent freeze
/// and re-freezes changed content onto a new tree, leaving the old common
/// tree untouched — the SDK marker-upgrade lifecycle.
#[test]
fn system_skills_marker_metadata_lifecycle() {
    let root = TempDir::new().expect("fixture root");
    let payload = b"system skill bytes\n".repeat(128);
    let store_root = store(root.path());
    let home_path = home(root.path(), "home", &payload, None, false);
    let first = match native_tree_cache::freeze(&store_root, &home_path, "skills/.system", &scope())
        .expect("freeze")
    {
        FreezeOutcome::Frozen(reference) => reference,
        other => panic!("unexpected outcome: {other:?}"),
    };
    let target = stdfs::read_link(home_path.join("skills/.system")).expect("link");
    let again = native_tree_cache::freeze(&store_root, &home_path, "skills/.system", &scope())
        .expect("idempotent freeze");
    assert_eq!(
        again,
        FreezeOutcome::AlreadyFrozen(first.clone()),
        "the same marker retains the link and common bytes"
    );
    assert_eq!(
        stdfs::read_link(home_path.join("skills/.system")).unwrap(),
        target
    );

    // A stale marker: the SDK replaced the link with new private bytes.
    stdfs::remove_file(home_path.join("skills/.system")).expect("drop link");
    system_skills(
        &home_path,
        &[
            ("skill.md", b"regenerated bytes\n"),
            ("nested/deep.md", b"x"),
        ],
    );
    let second =
        match native_tree_cache::freeze(&store_root, &home_path, "skills/.system", &scope())
            .expect("refreeze")
        {
            FreezeOutcome::Frozen(reference) => reference,
            other => panic!("unexpected outcome: {other:?}"),
        };
    assert_ne!(first.manifest_sha256, second.manifest_sha256);
    shared_assets::verify_shared_tree(&store_root, &first).expect("old tree unchanged");
    cleanup_store(&store_root);
}

/// Freeze and thaw never change the frozen index or history bytes.
#[test]
fn index_and_history_bytes_are_unchanged() {
    let root = TempDir::new().expect("fixture root");
    let payload = b"indexed home payload\n".repeat(64);
    let store_root = store(root.path());
    let home_path = home(root.path(), "home", &payload, Some(MARKER), true);
    let index = stdfs::read(home_path.join(RUNTIME_SNAPSHOT_INDEX)).expect("index");
    let history = stdfs::read(home_path.join("history.json")).expect("history");
    native_tree_cache::freeze(&store_root, &home_path, "skills/.system", &scope()).expect("freeze");
    native_tree_cache::freeze(
        &store_root,
        &home_path,
        "plugins/cache/fixture/probe",
        &scope(),
    )
    .expect("freeze");
    native_tree_cache::recover(&store_root, &home_path).expect("recover");
    assert_eq!(
        stdfs::read(home_path.join(RUNTIME_SNAPSHOT_INDEX)).unwrap(),
        index,
        "the frozen index bytes stay exact"
    );
    assert_eq!(
        stdfs::read(home_path.join("history.json")).unwrap(),
        history,
        "history stays untouched"
    );
    assert!(
        snapshot_tree::inspect_runtime_snapshots(&home_path, "revision-home", &fs::sha256(&index),)
            .expect("strict inspection")
            .verified,
        "the managed assets still strictly verify"
    );
    cleanup_store(&store_root);
}

/// Identical content under different compatibility domains never converges,
/// and a wrong-domain link is foreign.
#[test]
fn account_domains_stay_separated() {
    let root = TempDir::new().expect("fixture root");
    let payload = b"shared-looking payload\n".repeat(32);
    let store_root = store(root.path());
    let first = home(root.path(), "first", &payload, Some(MARKER), false);
    let second = home(root.path(), "second", &payload, Some(MARKER), false);
    let other = fs::sha256(b"native-cache-domain-b");
    let one = match native_tree_cache::freeze(
        &store_root,
        &first,
        "plugins/cache/fixture/probe",
        &scope(),
    )
    .expect("freeze")
    {
        FreezeOutcome::Frozen(reference) => reference,
        other => panic!("unexpected outcome: {other:?}"),
    };
    let two = match native_tree_cache::freeze(
        &store_root,
        &second,
        "plugins/cache/fixture/probe",
        &other,
    )
    .expect("freeze")
    {
        FreezeOutcome::Frozen(reference) => reference,
        other => panic!("unexpected outcome: {other:?}"),
    };
    assert_ne!(one.scope, two.scope);
    assert_ne!(
        stdfs::metadata(
            shared_assets::shared_tree_root(&store_root, &one)
                .unwrap()
                .join("1.0.0/payload.bin")
        )
        .unwrap()
        .ino(),
        stdfs::metadata(
            shared_assets::shared_tree_root(&store_root, &two)
                .unwrap()
                .join("1.0.0/payload.bin")
        )
        .unwrap()
        .ino(),
        "different domains never share payload inodes"
    );
    // Copying another account's link into a mismatched home is foreign only
    // when its scope differs from every domain this home froze under; the
    // module refuses by target shape, so a same-shape cross-mount must be
    // caught by the caller's domain policy. Here the shape check itself is
    // exercised: a link outside the store is foreign.
    let link = second.join("plugins/cache/fixture/probe");
    let target = stdfs::read_link(&link).expect("link");
    stdfs::remove_file(&link).expect("drop link");
    std::os::unix::fs::symlink("/etc", &link).expect("foreign link");
    assert!(
        native_tree_cache::thaw(&store_root, &second, "plugins/cache/fixture/probe").is_err(),
        "a foreign link is refused"
    );
    stdfs::remove_file(&link).expect("drop foreign link");
    std::os::unix::fs::symlink(&target, &link).expect("repair link");
    cleanup_store(&store_root);
}

/// Unsupported shapes, managed overlaps, and missing or invalid remote
/// markers are refused or skipped with the original preserved.
#[test]
fn unsupported_roots_and_overlaps_stay_untouched() {
    let root = TempDir::new().expect("fixture root");
    let payload = b"overlap payload\n".repeat(16);
    let store_root = store(root.path());

    // No marker: skipped unchanged.
    let unmarked = home(root.path(), "unmarked", &payload, None, false);
    assert_eq!(
        native_tree_cache::freeze(
            &store_root,
            &unmarked,
            "plugins/cache/fixture/probe",
            &scope(),
        )
        .expect("skip"),
        FreezeOutcome::SkippedUnchanged
    );
    // Invalid marker: skipped unchanged.
    stdfs::write(
        unmarked.join("plugins/cache/fixture/probe/.codex-remote-plugin-install.json"),
        "{\"schema_version\":2,\"remote_plugin_id\":\"\"}\n",
    )
    .expect("invalid marker");
    assert_eq!(
        native_tree_cache::freeze(
            &store_root,
            &unmarked,
            "plugins/cache/fixture/probe",
            &scope(),
        )
        .expect("skip"),
        FreezeOutcome::SkippedUnchanged
    );
    assert!(
        unmarked
            .join("plugins/cache/fixture/probe")
            .symlink_metadata()
            .unwrap()
            .is_dir(),
        "the skipped parent stays a real private tree"
    );

    // Indexed overlap: the system root is a managed indexed root. This home
    // is built without native system skills so the managed tree owns that
    // path outright.
    let indexed = root.path().join("indexed");
    stdfs::create_dir_all(&indexed).expect("home");
    remote_parent(&indexed, &payload, Some(MARKER));
    stdfs::write(indexed.join("history.json"), b"{}\n").expect("history");
    let source = root.path().join("indexed-managed");
    stdfs::create_dir_all(source.join("nested")).expect("nested source");
    stdfs::write(source.join("nested/file"), b"nested\n").expect("nested file");
    snapshot_tree::snapshot_managed_tree(&indexed, Path::new("skills/.system"), &source, None)
        .expect("indexed system root");
    snapshot_tree::finalize_runtime_snapshots(&indexed, "revision-indexed", &[], &[])
        .expect("index");
    assert!(
        native_tree_cache::freeze(&store_root, &indexed, "skills/.system", &scope()).is_err(),
        "an indexed root is refused"
    );
    assert!(
        indexed.join("skills/.system/nested/file").is_file(),
        "the managed tree is untouched"
    );

    // A symlink inside a candidate tree is an unsupported shape.
    let linked = home(root.path(), "linked", &payload, Some(MARKER), false);
    std::os::unix::fs::symlink(
        "/etc/hosts",
        linked.join("plugins/cache/fixture/probe/1.0.0/escape"),
    )
    .expect("internal symlink");
    assert!(
        native_tree_cache::freeze(
            &store_root,
            &linked,
            "plugins/cache/fixture/probe",
            &scope(),
        )
        .is_err(),
        "an internal symlink is refused"
    );
    assert!(
        linked
            .join("plugins/cache/fixture/probe/1.0.0")
            .symlink_metadata()
            .unwrap()
            .is_dir(),
        "the refused parent stays in place"
    );
    // The optional-cache failure is isolated: the system root still freezes.
    native_tree_cache::freeze(&store_root, &linked, "skills/.system", &scope())
        .expect("other roots keep working");
    cleanup_store(&store_root);
}

/// Every interrupted-freeze crash window recovers to a valid state without
/// losing the original.
#[test]
fn interrupted_freeze_states_recover() {
    let root = TempDir::new().expect("fixture root");
    let payload = b"recovery payload\n".repeat(64);
    let store_root = store(root.path());

    // Crash before the original moved: the record and staging exist, the
    // private tree is still in place; recovery drops the disposable backup.
    let stalled = home(root.path(), "stalled", &payload, Some(MARKER), false);
    let twin = home(root.path(), "twin", &payload, Some(MARKER), false);
    let reference = match native_tree_cache::freeze(
        &store_root,
        &twin,
        "plugins/cache/fixture/probe",
        &scope(),
    )
    .expect("twin freeze")
    {
        FreezeOutcome::Frozen(reference) => reference,
        other => panic!("unexpected outcome: {other:?}"),
    };
    let backup = stalled.join(".agent-run-native-abc123");
    stdfs::create_dir_all(backup.join("staging/plugins/cache/fixture/probe/1.0.0"))
        .expect("staging");
    stdfs::write(
        backup.join("staging/plugins/cache/fixture/probe/1.0.0/payload.bin"),
        &payload,
    )
    .expect("staged payload");
    stdfs::write(
        backup.join("op.json"),
        format!(
            "{{\"op_version\":1,\"op\":\"freeze\",\"root\":\"plugins/cache/fixture/probe\",\
             \"scope\":\"{}\",\"manifest_sha256\":\"{}\"}}",
            reference.scope, reference.manifest_sha256
        ),
    )
    .expect("record");
    native_tree_cache::recover(&store_root, &stalled).expect("recover pre-move window");
    assert!(!backup.exists(), "the disposable backup is dropped");
    assert!(
        stalled
            .join("plugins/cache/fixture/probe/1.0.0/payload.bin")
            .is_file(),
        "the original private tree survives"
    );

    // Crash between the move and the link: the root is missing, the backup
    // holds the original; recovery installs the exact link and cleans up.
    let moved = home(root.path(), "moved", &payload, Some(MARKER), false);
    let backup = moved.join(".agent-run-native-def456");
    stdfs::create_dir_all(backup.join("plugins/cache/fixture")).expect("backup chain");
    let parent = moved.join("plugins/cache/fixture/probe");
    stdfs::rename(&parent, backup.join("plugins/cache/fixture/probe")).expect("move original");
    stdfs::write(
        backup.join("op.json"),
        format!(
            "{{\"op_version\":1,\"op\":\"freeze\",\"root\":\"plugins/cache/fixture/probe\",\
             \"scope\":\"{}\",\"manifest_sha256\":\"{}\"}}",
            reference.scope, reference.manifest_sha256
        ),
    )
    .expect("record");
    native_tree_cache::recover(&store_root, &moved).expect("recover moved window");
    assert!(parent.symlink_metadata().unwrap().is_symlink());
    assert_eq!(
        stdfs::read_link(&parent).unwrap(),
        shared_assets::shared_tree_root(&store_root, &reference).unwrap()
    );
    assert!(!backup.exists());

    // A tampered backup is refused with everything left in place.
    let tampered = home(root.path(), "tampered", &payload, Some(MARKER), false);
    let backup = tampered.join(".agent-run-native-789012");
    stdfs::create_dir_all(backup.join("plugins/cache/fixture")).expect("backup chain");
    let parent = tampered.join("plugins/cache/fixture/probe");
    stdfs::rename(&parent, backup.join("plugins/cache/fixture/probe")).expect("move original");
    stdfs::write(
        backup.join("op.json"),
        format!(
            "{{\"op_version\":1,\"op\":\"freeze\",\"root\":\"plugins/cache/fixture/probe\",\
             \"scope\":\"{}\",\"manifest_sha256\":\"{}\"}}",
            reference.scope, reference.manifest_sha256
        ),
    )
    .expect("record");
    stdfs::write(
        backup.join("plugins/cache/fixture/probe/1.0.0/payload.bin"),
        b"tampered\n",
    )
    .expect("tamper");
    assert!(
        native_tree_cache::recover(&store_root, &tampered).is_err(),
        "an unproven backup is refused"
    );
    assert!(
        backup.exists(),
        "the tampered backup stays for the operator"
    );
    assert!(
        backup
            .join("plugins/cache/fixture/probe/1.0.0/payload.bin")
            .is_file(),
        "the original bytes are not lost"
    );
    cleanup_store(&store_root);
}

/// Every interrupted-thaw crash window recovers to a valid state.
#[test]
fn interrupted_thaw_states_recover() {
    let root = TempDir::new().expect("fixture root");
    let payload = b"thaw recovery payload\n".repeat(64);
    let store_root = store(root.path());
    let home_path = home(root.path(), "home", &payload, Some(MARKER), false);
    let root_key = "plugins/cache/fixture/probe";
    let reference = match native_tree_cache::freeze(&store_root, &home_path, root_key, &scope())
        .expect("freeze")
    {
        FreezeOutcome::Frozen(reference) => reference,
        other => panic!("unexpected outcome: {other:?}"),
    };
    let record = format!(
        "{{\"op_version\":1,\"op\":\"thaw\",\"root\":\"{root_key}\",\
         \"scope\":\"{}\",\"manifest_sha256\":\"{}\"}}",
        reference.scope, reference.manifest_sha256
    );

    // Crash after the clone was staged but before the link was dropped:
    // recovery rolls back to the exact link.
    let link = home_path.join(root_key);
    let target = stdfs::read_link(&link).expect("link");
    let backup = home_path.join(".agent-run-native-thaw1");
    stdfs::create_dir_all(backup.join("tree/1.0.0")).expect("staged clone");
    stdfs::write(backup.join("tree/1.0.0/payload.bin"), &payload).expect("clone payload");
    stdfs::write(backup.join("op.json"), &record).expect("record");
    native_tree_cache::recover(&store_root, &home_path).expect("recover pre-swap window");
    assert!(
        link.symlink_metadata().unwrap().is_symlink(),
        "link restored"
    );
    assert_eq!(stdfs::read_link(&link).unwrap(), target);
    assert!(!backup.exists());

    // Crash after the link was dropped but before the rename: recovery
    // completes the swap from the fully proven clone.
    stdfs::remove_file(&link).expect("drop link");
    let backup = home_path.join(".agent-run-native-thaw2");
    stage_complete_clone(&store_root, &reference, &backup);
    stdfs::write(backup.join("op.json"), &record).expect("record");
    native_tree_cache::recover(&store_root, &home_path).expect("recover swap window");
    assert!(
        link.symlink_metadata().unwrap().is_dir(),
        "the real private tree landed"
    );
    assert!(stdfs::read(link.join("1.0.0/payload.bin")).expect("restored payload") == payload);
    assert!(!backup.exists());

    // A record with no root and no clone rolls back to the link.
    stdfs::remove_dir_all(&link).expect("drop private tree");
    let backup = home_path.join(".agent-run-native-thaw3");
    stdfs::create_dir_all(&backup).expect("backup");
    stdfs::write(backup.join("op.json"), &record).expect("record");
    native_tree_cache::recover(&store_root, &home_path).expect("recover empty window");
    assert!(link.symlink_metadata().unwrap().is_symlink());
    assert_eq!(stdfs::read_link(&link).unwrap(), target);
    assert!(!backup.exists());
    // Missing records are explicit failures that preserve contents.
    let backup = home_path.join(".agent-run-native-thaw4");
    stdfs::create_dir_all(&backup).expect("backup");
    assert!(native_tree_cache::recover(&store_root, &home_path).is_err());
    assert!(backup.exists());
    cleanup_store(&store_root);
}

/// Stages one complete writable clone of a store tree's manifest entries
/// into `backup/tree`, as the module's own thaw would.
fn stage_complete_clone(store_root: &Path, reference: &SharedTreeRef, backup: &Path) {
    let tree = shared_assets::shared_tree_root(store_root, reference).unwrap();
    let manifest: serde_json::Value =
        serde_json::from_str(&stdfs::read_to_string(tree.join(SNAPSHOT_MANIFEST)).unwrap())
            .expect("manifest");
    for entry in manifest["entries"].as_array().expect("entries") {
        let relative = Path::new(entry["path"].as_str().expect("path"));
        let target = backup.join("tree").join(relative);
        if entry["type"] == "directory" {
            stdfs::create_dir_all(&target).expect("clone dir");
            continue;
        }
        stdfs::create_dir_all(target.parent().expect("parent")).expect("clone parent");
        stdfs::copy(tree.join(relative), &target).expect("clone file");
        let mode = entry["mode"].as_u64().expect("mode");
        stdfs::set_permissions(&target, stdfs::Permissions::from_mode(mode as u32))
            .expect("clone mode");
    }
}

/// The census pins every controlled link, pending backup, and backing blob,
/// and reports partial evidence instead of an empty set on read errors.
#[test]
fn ref_scan_pins_links_backups_and_blobs() {
    let root = TempDir::new().expect("fixture root");
    let payload = b"census payload\n".repeat(128);
    let store_root = store(root.path());
    let first = home(root.path(), "first", &payload, Some(MARKER), false);
    let second = home(root.path(), "second", &payload, Some(MARKER), false);
    let frozen = match native_tree_cache::freeze(
        &store_root,
        &first,
        "plugins/cache/fixture/probe",
        &scope(),
    )
    .expect("freeze")
    {
        FreezeOutcome::Frozen(reference) => reference,
        other => panic!("unexpected outcome: {other:?}"),
    };
    native_tree_cache::freeze(&store_root, &first, "skills/.system", &scope())
        .expect("system freeze");
    native_tree_cache::freeze(&store_root, &second, "skills/.system", &scope())
        .expect("second system freeze");
    let system = linked_reference(&store_root, &first, "skills/.system");

    let scan: NativeRefScan = native_tree_cache::scan_refs(&store_root, &[&first, &second]);
    assert!(scan.complete, "a fully readable pass is complete");
    assert_eq!(scan.homes, 2);
    assert!(
        scan.trees
            .contains_key(&format!("{}/{}", frozen.scope, frozen.manifest_sha256))
    );
    assert!(
        scan.trees
            .contains_key(&format!("{}/{}", system.scope, system.manifest_sha256))
    );
    let blobs = shared_assets::shared_tree_blob_names(&store_root, &frozen).unwrap();
    assert!(!blobs.is_empty());
    for blob in &blobs {
        assert!(scan.blobs.contains(blob), "census pins {blob:?}");
    }

    // A mid-freeze backup pins its tree before any link exists.
    let pending = root.path().join("pending");
    stdfs::create_dir_all(pending.join(".agent-run-native-mid")).expect("backup");
    stdfs::write(
        pending.join(".agent-run-native-mid/op.json"),
        format!(
            "{{\"op_version\":1,\"op\":\"freeze\",\"root\":\"skills/.system\",\
             \"scope\":\"{}\",\"manifest_sha256\":\"{}\"}}",
            frozen.scope, frozen.manifest_sha256
        ),
    )
    .expect("record");
    let scan = native_tree_cache::scan_refs(&store_root, &[&first, &pending]);
    assert!(scan.complete);
    assert_eq!(scan.backups, 1);
    assert!(
        scan.trees
            .contains_key(&format!("{}/{}", frozen.scope, frozen.manifest_sha256))
    );

    // An unreadable cache directory marks the pass incomplete but keeps the
    // prior home's refs — an enumeration error is never an empty census.
    stdfs::set_permissions(
        second.join("plugins/cache"),
        stdfs::Permissions::from_mode(0o000),
    )
    .expect("unreadable");
    let scan = native_tree_cache::scan_refs(&store_root, &[&first, &second]);
    assert!(!scan.complete, "a read failure is never an empty answer");
    assert_eq!(scan.homes, 2, "the failing home still counts as examined");
    assert!(!scan.trees.is_empty(), "partial evidence is preserved");
    stdfs::set_permissions(
        second.join("plugins/cache"),
        stdfs::Permissions::from_mode(0o755),
    )
    .expect("repair");
    cleanup_store(&store_root);
}

/// A census page far beyond the former 512-home ceiling completes for many
/// simple homes, and callers can page and merge larger sets.
#[test]
fn census_pages_beyond_five_hundred_homes() {
    let root = TempDir::new().expect("fixture root");
    let store_root = store(root.path());
    let mut homes: Vec<PathBuf> = Vec::new();
    for index in 0..600 {
        let home = root.path().join(format!("home-{index:03}"));
        stdfs::create_dir_all(&home).expect("home");
        homes.push(home);
    }
    let references: Vec<&Path> = homes.iter().map(PathBuf::as_path).collect();
    let scan = native_tree_cache::scan_refs(&store_root, &references);
    assert!(
        scan.complete,
        "six hundred simple homes are one complete page"
    );
    assert_eq!(scan.homes, 600);

    let mut merged = native_tree_cache::scan_refs(&store_root, &references[..300]);
    let page = native_tree_cache::scan_refs(&store_root, &references[300..]);
    assert!(merged.complete && page.complete);
    merged.merge(page);
    assert_eq!(merged.homes, 600);
    assert!(merged.complete, "merged pages keep completeness");
    // A home holding one frozen link survives paging with its ref pinned.
    let payload = b"paged payload
"
    .repeat(8);
    let active = home(root.path(), "active", &payload, None, false);
    native_tree_cache::freeze(&store_root, &active, "skills/.system", &scope()).expect("freeze");
    let mut merged = native_tree_cache::scan_refs(&store_root, &[&active]);
    let page = native_tree_cache::scan_refs(&store_root, &references[..10]);
    merged.merge(page);
    assert!(merged.complete);
    assert_eq!(merged.homes, 11);
    assert_eq!(merged.trees.len(), 1, "the frozen link stays pinned");
    cleanup_store(&store_root);
}

/// Native selector: the real codex binary keeps a synthetic remote-marked
/// plugin discoverable through the frozen link and after thaw.
///
/// Ignored by default: it shells out to the real `codex` binary (`CODEX_BIN`
/// or `PATH`) with an owned fixture home, a synthetic valid remote marker,
/// and no credentials; commands are metadata-only (no model turns).
#[test]
#[ignore = "runs the real codex binary; select it with CODEX_BIN"]
fn native_codex_discovers_frozen_and_thawed_cache() {
    let codex = || {
        std::env::var_os("CODEX_BIN").map_or_else(
            || Command::new("codex"),
            |binary| Command::new(PathBuf::from(binary)),
        )
    };
    let root = TempDir::new().expect("fixture root");
    let market = root.path().join("market");
    stdfs::create_dir_all(market.join(".agents/plugins")).expect("marketplace dir");
    stdfs::create_dir_all(market.join("plugins/probe/.codex-plugin")).expect("plugin dir");
    stdfs::create_dir_all(market.join("plugins/probe/skills/probe")).expect("skill dir");
    stdfs::write(
        market.join(".agents/plugins/marketplace.json"),
        "{\"name\":\"fixture\",\"interface\":{\"displayName\":\"fixture\"},\"plugins\":[{\"name\":\"probe\",\"source\":{\"source\":\"local\",\"path\":\"./plugins/probe\"},\"policy\":{\"installation\":\"AVAILABLE\",\"authentication\":\"ON_INSTALL\"}}]}",
    )
    .expect("marketplace");
    stdfs::write(
        market.join("plugins/probe/.codex-plugin/plugin.json"),
        "{\"name\":\"probe\",\"version\":\"1.0.0\",\"description\":\"fixture\",\"skills\":\"./skills\"}",
    )
    .expect("plugin manifest");
    stdfs::write(
        market.join("plugins/probe/skills/probe/SKILL.md"),
        "---\ndescription: ar-native-cache-marker\n---\n# probe\n",
    )
    .expect("skill");
    let home_path = root.path().join("home");
    stdfs::create_dir_all(&home_path).expect("home");
    let mut install = codex();
    install
        .env("CODEX_HOME", &home_path)
        .env("HOME", root.path())
        .args(["plugin", "marketplace", "add"])
        .arg(&market)
        .arg("--json");
    let output = run_bounded(&mut install);
    assert!(
        output.status.success(),
        "native marketplace add failed (booleans only, no output dump)"
    );
    let mut add = codex();
    add.env("CODEX_HOME", &home_path)
        .env("HOME", root.path())
        .args(["plugin", "add", "probe@fixture", "--json"]);
    let output = run_bounded(&mut add);
    assert!(
        output.status.success(),
        "native plugin add failed (booleans only, no output dump)"
    );
    // The downloaded parent gains the synthetic schema-1 remote marker this
    // unit requires; local-marketplace installs do not write one.
    stdfs::write(
        home_path.join("plugins/cache/fixture/probe/.codex-remote-plugin-install.json"),
        MARKER,
    )
    .expect("synthetic marker");

    let store_root = store(root.path());
    let root_key = "plugins/cache/fixture/probe";
    native_tree_cache::freeze(&store_root, &home_path, root_key, &scope()).expect("freeze");
    let installed_through_link = |home: &Path| {
        let mut command = codex();
        command
            .env("CODEX_HOME", home)
            .env("HOME", root.path())
            .args(["plugin", "list", "--json"]);
        let output = run_bounded(&mut command);
        assert!(
            output.status.success(),
            "codex plugin list failed (booleans only)"
        );
        let document: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("plugin list JSON");
        document["installed"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .any(|entry| {
                entry["pluginId"] == "probe@fixture"
                    && entry["installed"] == serde_json::json!(true)
            })
    };
    assert!(
        installed_through_link(&home_path),
        "native discovery reads through the frozen whole-root link"
    );
    native_tree_cache::thaw(&store_root, &home_path, root_key)
        .expect("thaw")
        .expect("thawed");
    assert!(
        installed_through_link(&home_path),
        "native discovery reads the thawed private tree"
    );
    cleanup_store(&store_root);
}

/// Runs one owned child to completion under a hard 15-second bound.
fn run_bounded(command: &mut Command) -> std::process::Output {
    let mut child = command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn codex");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(50)),
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("codex exceeded the 15-second bound");
            }
            Err(error) => panic!("codex could not be reaped: {error}"),
        }
    }
    child.wait_with_output().expect("collect codex output")
}

/// Returns a relative path from directory `from` to `to`, for building
/// links that resolve equivalently to an absolute target.
fn relative_to(from: &Path, to: &Path) -> PathBuf {
    let from: Vec<_> = from.components().collect();
    let to: Vec<_> = to.components().collect();
    let shared = from.iter().zip(&to).take_while(|(a, b)| a == b).count();
    let mut relative = PathBuf::new();
    for _ in shared..from.len() {
        relative.push("..");
    }
    for component in &to[shared..] {
        relative.push(component.as_os_str());
    }
    relative
}

/// The aggregate byte bound spans the whole capture: small files split
/// across two subdirectories whose sum exceeds the bound are refused before
/// anything is published, and the originals remain.
#[test]
fn aggregate_bound_spans_subdirectories() {
    let root = TempDir::new().expect("fixture root");
    let store_root = store(root.path());
    let home_path = root.path().join("oversized");
    stdfs::create_dir_all(&home_path).expect("home");
    // Nine sparse 15 MiB files across two subdirectories: each file is far
    // below the 16 MiB per-file bound while the tree totals ~135 MiB, above
    // the 128 MiB aggregate bound. Sparse extension keeps the fixture cheap.
    let per_file: u64 = 15 * 1024 * 1024;
    for (subdir, count) in [("a", 5), ("b", 4)] {
        for index in 0..count {
            let path = home_path
                .join("skills/.system")
                .join(subdir)
                .join(format!("part-{index:02}.bin"));
            stdfs::create_dir_all(path.parent().expect("parent")).expect("dir");
            let file = stdfs::File::create(&path).expect("sparse file");
            file.set_len(per_file).expect("sparse length");
        }
    }
    let error = native_tree_cache::freeze(&store_root, &home_path, "skills/.system", &scope())
        .expect_err("the aggregate bound refuses the tree");
    assert!(
        error.to_string().contains("aggregate"),
        "the refusal names the aggregate bound: {error}"
    );
    assert!(
        !store_root.join("trees").exists(),
        "nothing was published into the store"
    );
    assert!(
        home_path.join("skills/.system/b/part-03.bin").is_file(),
        "every original file remains"
    );
    assert!(
        stdfs::read_dir(&home_path)
            .expect("home listing")
            .flatten()
            .all(|entry| !entry
                .file_name()
                .to_string_lossy()
                .starts_with(".agent-run-native-")),
        "no operation backup was left behind"
    );
    cleanup_store(&store_root);
}

/// An unrecognized link in a supported cache slot — a relative spelling
/// resolving onto the very tree another home froze, or a foreign absolute
/// target — marks the census incomplete instead of proving no reference;
/// the link itself is never followed or rewritten.
#[test]
fn unrecognized_links_mark_census_incomplete() {
    let root = TempDir::new().expect("fixture root");
    let payload = b"unknown evidence payload\n".repeat(32);
    let store_root = store(root.path());
    let home_path = home(root.path(), "home", &payload, Some(MARKER), false);
    let root_key = "plugins/cache/fixture/probe";
    native_tree_cache::freeze(&store_root, &home_path, root_key, &scope()).expect("freeze");
    let link = home_path.join(root_key);
    let exact = stdfs::read_link(&link).expect("exact link");

    // A relative spelling resolving onto the very same store tree.
    let relative = relative_to(&home_path.join("plugins/cache/fixture"), &exact);
    assert!(relative.is_relative());
    stdfs::remove_file(&link).expect("drop link");
    std::os::unix::fs::symlink(&relative, &link).expect("relative link");
    let scan = native_tree_cache::scan_refs(&store_root, &[&home_path]);
    assert!(
        !scan.complete,
        "an unrecognized link is unknown reference evidence, not no reference"
    );
    assert!(
        scan.trees.is_empty(),
        "an unrecognized link proves no reference either; retention comes\
         \nfrom complete == false, never from an invented ref"
    );

    // A foreign absolute target in a supported slot is equally conservative.
    stdfs::remove_file(&link).expect("drop link");
    std::os::unix::fs::symlink("/etc", &link).expect("foreign link");
    let scan = native_tree_cache::scan_refs(&store_root, &[&home_path]);
    assert!(!scan.complete);
    assert!(
        link.symlink_metadata().unwrap().is_symlink(),
        "the foreign link itself stays untouched"
    );

    // The exact controlled spelling restores complete evidence with the ref.
    stdfs::remove_file(&link).expect("drop link");
    std::os::unix::fs::symlink(&exact, &link).expect("repair link");
    let scan = native_tree_cache::scan_refs(&store_root, &[&home_path]);
    assert!(scan.complete);
    assert_eq!(scan.trees.len(), 1);
    cleanup_store(&store_root);
}

/// Home-relative curated working-tree root.
const MIRROR: &str = ".tmp/plugins/plugins";
/// Home-relative curated Git pack root.
const PACKS: &str = ".tmp/plugins/.git/objects/pack";

/// Returns the frozen reference of one freeze outcome, panicking otherwise.
fn frozen(outcome: FreezeOutcome) -> SharedTreeRef {
    match outcome {
        FreezeOutcome::Frozen(reference) | FreezeOutcome::AlreadyFrozen(reference) => reference,
        other => panic!("unexpected outcome: {other:?}"),
    }
}

/// Reads one shared tree's manifest bytes and entry count.
fn manifest_of(store_root: &Path, reference: &SharedTreeRef) -> (usize, usize) {
    let bytes = stdfs::read(
        shared_assets::shared_tree_root(store_root, reference)
            .unwrap()
            .join(SNAPSHOT_MANIFEST),
    )
    .expect("manifest");
    let document: serde_json::Value = serde_json::from_slice(&bytes).expect("manifest json");
    (bytes.len(), document["entries"].as_array().unwrap().len())
}

/// Writes the measured `data-analytics`-shaped remote plugin parent: 704
/// files and 149 directories (853 entries), depth 7, with a valid marker.
fn large_remote_parent(home: &Path) -> &'static str {
    let root_key = "plugins/cache/openai-curated/data-analytics";
    let parent = home.join(root_key);
    let mut directories = Vec::new();
    for branch in 0..21 {
        let mut chain = parent.join(format!("1.0.0/branch-{branch:02}"));
        for depth in 0..7 {
            if depth > 0 {
                chain = chain.join(format!("d{depth}"));
            }
            directories.push(chain.clone());
        }
    }
    directories.push(parent.join("1.0.0/extra-a"));
    for directory in &directories {
        stdfs::create_dir_all(directory).expect("parent directory");
    }
    for index in 0..703 {
        let directory = &directories[index % directories.len()];
        stdfs::write(
            directory.join(format!("asset-{index:04}.md")),
            format!("data analytics asset {index}\n").repeat(4),
        )
        .expect("parent file");
    }
    stdfs::write(parent.join(".codex-remote-plugin-install.json"), MARKER).expect("marker");
    root_key
}

/// Real-sized curated working tree (7732 entries, depth 10, manifest far
/// beyond 64 KiB) and the 853-entry remote parent freeze, verify, thaw
/// back to exact private bytes and refreeze onto the same tree identity,
/// while every private Git, `.agents`, root and sibling file keeps its
/// bytes, inode and modification time.
#[test]
fn measured_curated_mirror_and_large_parent_round_trip() {
    let root = TempDir::new().expect("fixture root");
    let store_root = store(root.path());
    let home_path = root.path().join("home");
    let private = common::curated_clone(&home_path, &common::measured_shape(48, 4096), "home-a");
    let parent_key = large_remote_parent(&home_path);
    let home_path = home_path.canonicalize().unwrap();
    let before = common::private_identity(&private);
    let sample = home_path
        .join(MIRROR)
        .join("plugin-07/level-1/level-2/level-3/level-4/level-5/level-6/level-7/level-8/level-9");
    let sample_names: Vec<_> = stdfs::read_dir(&sample)
        .unwrap()
        .flatten()
        .map(|e| e.file_name())
        .collect();
    assert!(
        !sample_names.is_empty(),
        "the depth-10 directory holds files"
    );

    let mut references = Vec::new();
    for root_key in [MIRROR, PACKS, parent_key] {
        let reference = frozen(
            native_tree_cache::freeze(&store_root, &home_path, root_key, &scope())
                .unwrap_or_else(|error| panic!("{root_key}: {error}")),
        );
        shared_assets::verify_shared_tree(&store_root, &reference).expect("verified");
        assert!(
            home_path
                .join(root_key)
                .symlink_metadata()
                .unwrap()
                .is_symlink()
        );
        references.push(reference);
    }
    let (mirror_bytes, mirror_entries) = manifest_of(&store_root, &references[0]);
    assert!(mirror_entries == 7732, "{mirror_entries}");
    assert!(mirror_bytes > 64 * 1024, "{mirror_bytes}");
    let (parent_bytes, parent_entries) = manifest_of(&store_root, &references[2]);
    assert!(
        parent_entries >= 853 && parent_bytes > 64 * 1024,
        "{parent_entries} {parent_bytes}"
    );
    assert_eq!(
        common::private_identity(&private),
        before,
        "private state untouched by freeze"
    );

    for (root_key, reference) in [MIRROR, PACKS, parent_key].into_iter().zip(&references) {
        let thawed = native_tree_cache::thaw(&store_root, &home_path, root_key)
            .expect("thaw")
            .expect("was frozen");
        assert_eq!(&thawed, reference);
        assert!(
            home_path
                .join(root_key)
                .symlink_metadata()
                .unwrap()
                .is_dir()
        );
    }
    let restored = home_path
        .join(MIRROR)
        .join("plugin-07/level-1/level-2/level-3/level-4/level-5/level-6/level-7/level-8/level-9");
    for name in &sample_names {
        let bytes = stdfs::read(restored.join(name)).unwrap();
        assert!(
            bytes.starts_with(b"curated file "),
            "exact private bytes return"
        );
    }
    assert_eq!(
        stdfs::metadata(
            home_path
                .join(PACKS)
                .join(format!("{}.pack", common::PACK_STEM))
        )
        .unwrap()
        .len(),
        4096
    );
    assert_eq!(
        common::private_identity(&private),
        before,
        "private state untouched by thaw"
    );
    // The source tree identity is stable across a freeze/thaw round trip.
    for (root_key, reference) in [MIRROR, PACKS, parent_key].into_iter().zip(&references) {
        let again = frozen(
            native_tree_cache::freeze(&store_root, &home_path, root_key, &scope())
                .expect("refreeze"),
        );
        assert_eq!(&again, reference, "{root_key} keeps its tree identity");
    }
    cleanup_store(&store_root);
}

/// Two independent homes with byte-identical curated clones share one pack
/// payload inode (24 MB, within the 32 MiB stream bound) and one working
/// tree, while each home's private Git state keeps its own bytes, inode and
/// time; the private duplicate of the pack is measurably gone.
#[test]
fn identical_curated_clones_share_packs_and_keep_git_private() {
    let root = TempDir::new().expect("fixture root");
    let store_root = store(root.path());
    let shape = common::curated_shape_small(common::MEASURED_PACK_BYTES);
    let first = root.path().join("first");
    let second = root.path().join("second");
    let private_first = common::curated_clone(&first, &shape, "first");
    let private_second = common::curated_clone(&second, &shape, "second");
    let (first, second) = (
        first.canonicalize().unwrap(),
        second.canonicalize().unwrap(),
    );
    let before_first = common::private_identity(&private_first);
    let before_second = common::private_identity(&private_second);
    let original = unique_inode_bytes(&[first.clone(), second.clone()]);

    let mut targets = Vec::new();
    for home_path in [&first, &second] {
        let refs: Vec<SharedTreeRef> = [MIRROR, PACKS]
            .into_iter()
            .map(|root_key| {
                frozen(
                    native_tree_cache::freeze(&store_root, home_path, root_key, &scope())
                        .expect("freeze"),
                )
            })
            .collect();
        targets.push(refs);
    }
    assert_eq!(
        targets[0], targets[1],
        "identical clones converge on the same trees"
    );
    let pack = |home: &Path| {
        stdfs::metadata(home.join(PACKS).join(format!("{}.pack", common::PACK_STEM))).unwrap()
    };
    assert_eq!(
        pack(&first).ino(),
        pack(&second).ino(),
        "one shared pack payload inode"
    );
    assert_eq!(pack(&first).len(), common::MEASURED_PACK_BYTES);
    let idle = unique_inode_bytes(&[first.clone(), second.clone(), store_root.clone()]);
    assert!(
        original - idle >= common::MEASURED_PACK_BYTES,
        "one private pack duplicate is gone: {original} -> {idle}"
    );
    assert_eq!(common::private_identity(&private_first), before_first);
    assert_eq!(common::private_identity(&private_second), before_second);
    assert_ne!(
        before_first[1].1, before_second[1].1,
        "private Git state stays per home"
    );
    cleanup_store(&store_root);
}

/// A pack beyond the 32 MiB streaming bound is refused with the original
/// pack directory untouched and nothing published.
#[test]
fn curated_pack_beyond_the_stream_bound_is_refused() {
    let root = TempDir::new().expect("fixture root");
    let store_root = store(root.path());
    let home_path = root.path().join("home");
    common::curated_clone(
        &home_path,
        &common::curated_shape_small(32 * 1024 * 1024 + 1),
        "big",
    );
    let home_path = home_path.canonicalize().unwrap();
    let error = native_tree_cache::freeze(&store_root, &home_path, PACKS, &scope())
        .expect_err("the stream bound refuses the pack");
    assert!(error.to_string().contains("payload bound"), "{error}");
    assert!(home_path.join(PACKS).symlink_metadata().unwrap().is_dir());
    assert_eq!(
        stdfs::metadata(
            home_path
                .join(PACKS)
                .join(format!("{}.pack", common::PACK_STEM))
        )
        .unwrap()
        .len(),
        32 * 1024 * 1024 + 1
    );
    assert!(!store_root.join("trees").exists(), "nothing was published");
    cleanup_store(&store_root);
}

/// One named fixture mutation applied to a fresh curated clone.
type Case = (&'static str, fn(&Path));

/// Curated roots outside the native clone shape stay private and unchanged:
/// no `.git`, a gitfile `.git`, an empty pack directory, and a pack
/// directory holding a multi-pack index or a temporary pack.
#[test]
fn curated_shapes_outside_the_native_clone_stay_private() {
    let root = TempDir::new().expect("fixture root");
    let store_root = store(root.path());
    let shape = common::curated_shape_small(64);
    let cases: [Case; 5] = [
        ("no-git", |home| {
            stdfs::remove_dir_all(home.join(".tmp/plugins/.git")).unwrap()
        }),
        ("gitfile", |home| {
            stdfs::remove_dir_all(home.join(".tmp/plugins/.git")).unwrap();
            stdfs::write(home.join(".tmp/plugins/.git"), "gitdir: /elsewhere\n").unwrap();
        }),
        ("empty-pack", |home| {
            stdfs::remove_dir_all(home.join(PACKS)).unwrap();
            stdfs::create_dir_all(home.join(PACKS)).unwrap();
        }),
        ("multi-pack-index", |home| {
            stdfs::write(home.join(PACKS).join("multi-pack-index"), b"MIDX").unwrap()
        }),
        ("tmp-pack", |home| {
            stdfs::write(home.join(PACKS).join("tmp_pack_x1"), b"x").unwrap()
        }),
    ];
    for (name, mutate) in cases {
        let home_path = root.path().join(name);
        common::curated_clone(&home_path, &shape, name);
        mutate(&home_path);
        let home_path = home_path.canonicalize().unwrap();
        for root_key in [MIRROR, PACKS] {
            if !home_path.join(root_key).exists() {
                continue;
            }
            let outcome = native_tree_cache::freeze(&store_root, &home_path, root_key, &scope())
                .unwrap_or_else(|error| panic!("{name} {root_key}: {error}"));
            let expect_frozen = matches!(name, "empty-pack" | "multi-pack-index" | "tmp-pack")
                && root_key == MIRROR;
            if expect_frozen {
                frozen(outcome);
                native_tree_cache::thaw(&store_root, &home_path, root_key).expect("thaw back");
            } else {
                assert_eq!(
                    outcome,
                    FreezeOutcome::SkippedUnchanged,
                    "{name} {root_key}"
                );
            }
            assert!(
                home_path
                    .join(root_key)
                    .symlink_metadata()
                    .unwrap()
                    .is_dir(),
                "{name}"
            );
        }
    }
    cleanup_store(&store_root);
}

/// Interrupted curated freezes and thaws recover through the same op.json
/// records: a moved pack original relinks, and a staged mirror clone whose
/// link was never dropped rolls back to the exact link.
#[test]
fn interrupted_curated_operations_recover() {
    let root = TempDir::new().expect("fixture root");
    let store_root = store(root.path());
    let shape = common::curated_shape_small(8192);
    let twin = root.path().join("twin");
    common::curated_clone(&twin, &shape, "twin");
    let twin = twin.canonicalize().unwrap();
    let pack_ref =
        frozen(native_tree_cache::freeze(&store_root, &twin, PACKS, &scope()).expect("twin packs"));
    let mirror_ref = frozen(
        native_tree_cache::freeze(&store_root, &twin, MIRROR, &scope()).expect("twin mirror"),
    );

    // Freeze crash between the move and the link, on the pack root.
    let moved = root.path().join("moved");
    let private = common::curated_clone(&moved, &shape, "moved");
    let moved = moved.canonicalize().unwrap();
    let before = common::private_identity(&private);
    let backup = moved.join(".agent-run-native-cur1");
    stdfs::create_dir_all(backup.join(".tmp/plugins/.git/objects")).expect("backup chain");
    stdfs::rename(moved.join(PACKS), backup.join(PACKS)).expect("move original");
    stdfs::write(
        backup.join("op.json"),
        format!(
            "{{\"op_version\":1,\"op\":\"freeze\",\"root\":\"{PACKS}\",\"scope\":\"{}\",\"manifest_sha256\":\"{}\"}}",
            pack_ref.scope, pack_ref.manifest_sha256
        ),
    )
    .expect("record");
    native_tree_cache::recover(&store_root, &moved).expect("recover moved pack");
    assert_eq!(
        stdfs::read_link(moved.join(PACKS)).unwrap(),
        shared_assets::shared_tree_root(&store_root, &pack_ref).unwrap()
    );
    assert!(!backup.exists());
    assert_eq!(
        common::private_identity(&private),
        before,
        "recovery keeps Git state"
    );

    // Thaw crash after the clone was staged but before the link dropped.
    let link = twin.join(MIRROR);
    let target = stdfs::read_link(&link).unwrap();
    let backup = twin.join(".agent-run-native-cur2");
    stdfs::create_dir_all(backup.join("tree/plugin-00")).expect("staged clone");
    stdfs::write(backup.join("tree/plugin-00/partial.md"), b"partial").expect("partial clone");
    stdfs::write(
        backup.join("op.json"),
        format!(
            "{{\"op_version\":1,\"op\":\"thaw\",\"root\":\"{MIRROR}\",\"scope\":\"{}\",\"manifest_sha256\":\"{}\"}}",
            mirror_ref.scope, mirror_ref.manifest_sha256
        ),
    )
    .expect("record");
    native_tree_cache::recover(&store_root, &twin).expect("recover staged thaw");
    assert_eq!(
        stdfs::read_link(&link).unwrap(),
        target,
        "rolled back to the exact link"
    );
    assert!(!backup.exists());
    cleanup_store(&store_root);
}

/// A crash while a proven freeze original is being removed leaves a
/// partial `discard` subtree beside the record; recovery removes it without
/// re-proving, keeps the exact link, and leaves private Git state alone.
#[test]
fn interrupted_discard_recovers_without_reproof() {
    let root = TempDir::new().expect("fixture root");
    let store_root = store(root.path());
    let home_path = root.path().join("home");
    let private = common::curated_clone(&home_path, &common::curated_shape_small(4096), "home");
    let home_path = home_path.canonicalize().unwrap();
    let before = common::private_identity(&private);
    let reference = frozen(
        native_tree_cache::freeze(&store_root, &home_path, MIRROR, &scope()).expect("freeze"),
    );
    let backup = home_path.join(".agent-run-native-disc1");
    stdfs::create_dir_all(backup.join("discard/plugin-00/level-1")).expect("partial discard");
    stdfs::write(
        backup.join("discard/plugin-00/level-1/leftover.md"),
        b"half removed",
    )
    .expect("leftover");
    stdfs::write(
        backup.join("op.json"),
        format!(
            "{{\"op_version\":1,\"op\":\"freeze\",\"root\":\"{MIRROR}\",\"scope\":\"{}\",\"manifest_sha256\":\"{}\"}}",
            reference.scope, reference.manifest_sha256
        ),
    )
    .expect("record");
    native_tree_cache::recover(&store_root, &home_path).expect("recover partial discard");
    assert!(
        !backup.exists(),
        "the partial discard and its record are gone"
    );
    assert_eq!(
        stdfs::read_link(home_path.join(MIRROR)).unwrap(),
        shared_assets::shared_tree_root(&store_root, &reference).unwrap()
    );
    assert_eq!(common::private_identity(&private), before);
    cleanup_store(&store_root);
}

/// The census pins curated links and their blobs while any home links them
/// and drops them once the last physical home is gone.
#[test]
fn census_pins_curated_links_until_the_last_home_goes() {
    let root = TempDir::new().expect("fixture root");
    let store_root = store(root.path());
    let shape = common::curated_shape_small(4096);
    let mut homes = Vec::new();
    let mut references = Vec::new();
    for name in ["first", "second"] {
        let home_path = root.path().join(name);
        common::curated_clone(&home_path, &shape, name);
        let home_path = home_path.canonicalize().unwrap();
        references = [MIRROR, PACKS]
            .into_iter()
            .map(|root_key| {
                frozen(
                    native_tree_cache::freeze(&store_root, &home_path, root_key, &scope()).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        homes.push(home_path);
    }
    let keys: Vec<String> = references
        .iter()
        .map(|reference| format!("{}/{}", reference.scope, reference.manifest_sha256))
        .collect();
    let scan = |homes: &[&Path]| native_tree_cache::scan_refs(&store_root, homes);
    let both = scan(&[&homes[0], &homes[1]]);
    assert!(both.complete);
    assert!(keys.iter().all(|key| both.trees.contains_key(key)));
    let pack_blobs = shared_assets::shared_tree_blob_names(&store_root, &references[1]).unwrap();
    assert!(pack_blobs.iter().all(|blob| both.blobs.contains(blob)));
    stdfs::remove_dir_all(&homes[0]).unwrap();
    let last = scan(&[&homes[1]]);
    assert!(last.complete && keys.iter().all(|key| last.trees.contains_key(key)));
    stdfs::remove_dir_all(&homes[1]).unwrap();
    let none = scan(&[]);
    assert!(none.complete && none.trees.is_empty() && none.blobs.is_empty());
    cleanup_store(&store_root);
}

/// Concurrent starts over two homes with identical clones converge on one
/// tree per root and thaw into independent private inodes.
#[test]
fn concurrent_curated_freezes_and_thaws_stay_independent() {
    let root = TempDir::new().expect("fixture root");
    let store_root = store(root.path());
    let shape = common::curated_shape_small(8192);
    let homes: Vec<PathBuf> = ["left", "right"]
        .into_iter()
        .map(|name| {
            let home_path = root.path().join(name);
            common::curated_clone(&home_path, &shape, name);
            home_path.canonicalize().unwrap()
        })
        .collect();
    let refs: Vec<Vec<SharedTreeRef>> = std::thread::scope(|scope_| {
        let workers: Vec<_> = homes
            .iter()
            .map(|home_path| {
                let store_root = &store_root;
                scope_.spawn(move || {
                    [MIRROR, PACKS]
                        .into_iter()
                        .map(|root_key| {
                            frozen(
                                native_tree_cache::freeze(
                                    store_root,
                                    home_path,
                                    root_key,
                                    &scope(),
                                )
                                .unwrap(),
                            )
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect()
    });
    assert_eq!(refs[0], refs[1]);
    std::thread::scope(|scope_| {
        for home_path in &homes {
            let store_root = &store_root;
            scope_.spawn(move || {
                for root_key in [PACKS, MIRROR] {
                    native_tree_cache::thaw(store_root, home_path, root_key).unwrap();
                }
            });
        }
    });
    let inode = |home: &Path| {
        stdfs::metadata(home.join(PACKS).join(format!("{}.pack", common::PACK_STEM)))
            .unwrap()
            .ino()
    };
    assert_ne!(
        inode(&homes[0]),
        inode(&homes[1]),
        "thawed packs are independent"
    );
    for home_path in &homes {
        assert!(home_path.join(MIRROR).symlink_metadata().unwrap().is_dir());
        assert!(home_path.join(PACKS).symlink_metadata().unwrap().is_dir());
    }
    cleanup_store(&store_root);
}

/// Measures the curated increment on two homes with the measured native
/// shape: 5380 files totalling ~53.6 MB in 2352 directories (depth 10) plus
/// the 24,205,581-byte pack. Prints unique-inode bytes before, idle after
/// both freezes, peak with one home thawed, and idle again after refreeze,
/// plus first/second freeze, thaw and refreeze wall times.
/// Run with `cargo test --release ... -- --ignored --nocapture`.
#[test]
#[ignore = "measurement; run in release with --ignored --nocapture"]
fn measure_curated_increment() {
    let root = TempDir::new().expect("fixture root");
    let store_root = store(root.path());
    let shape = common::measured_shape(53_611_752 / 5380, common::MEASURED_PACK_BYTES);
    let homes: Vec<PathBuf> = ["first", "second"]
        .into_iter()
        .map(|name| {
            let home_path = root.path().join(name);
            common::curated_clone(&home_path, &shape, name);
            home_path.canonicalize().unwrap()
        })
        .collect();
    let bytes = |paths: &[PathBuf]| unique_inode_bytes(paths);
    let all = [homes[0].clone(), homes[1].clone(), store_root.clone()];
    let before = bytes(&all);
    let timed = |label: &str, work: &mut dyn FnMut()| {
        let started = Instant::now();
        work();
        let elapsed = started.elapsed();
        eprintln!("measure {label}: {:.3}s", elapsed.as_secs_f64());
        elapsed
    };
    for (index, home_path) in homes.iter().enumerate() {
        timed(&format!("freeze home {}", index + 1), &mut || {
            for root_key in [MIRROR, PACKS] {
                frozen(
                    native_tree_cache::freeze(&store_root, home_path, root_key, &scope()).unwrap(),
                );
            }
        });
    }
    let idle = bytes(&all);
    let thaw = timed("thaw home 1 (mirror+packs)", &mut || {
        for root_key in [PACKS, MIRROR] {
            native_tree_cache::thaw(&store_root, &homes[0], root_key).unwrap();
        }
    });
    let peak = bytes(&all);
    timed("refreeze home 1 unchanged", &mut || {
        for root_key in [MIRROR, PACKS] {
            frozen(native_tree_cache::freeze(&store_root, &homes[0], root_key, &scope()).unwrap());
        }
    });
    let after = bytes(&all);
    eprintln!(
        "measure unique-inode bytes: before={before} idle={idle} peak(one thawed)={peak} idle-after={after}"
    );
    eprintln!("measure thaw: {:.3}s", thaw.as_secs_f64());
    cleanup_store(&store_root);
}
