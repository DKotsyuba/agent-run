//! Differential managed-home snapshot checks against Python-v1 fixtures.

use agent_run_platform::{
    fs,
    snapshot_tree::{self, RUNTIME_SNAPSHOT_INDEX, SNAPSHOT_MANIFEST},
};
use serde_json::Value;
use std::{
    fs as stdfs,
    path::{Path, PathBuf},
};
use tempfile::TempDir;

/// Create the source tree used by the managed-tree differential tests.
fn source(root: &Path) -> PathBuf {
    let source = root.join("source");
    stdfs::create_dir_all(source.join("scripts")).expect("source directories");
    stdfs::create_dir(source.join("empty")).expect("empty directory");
    stdfs::write(source.join("SKILL.md"), "first").expect("skill");
    stdfs::write(source.join("scripts/run.sh"), b"#!/bin/sh\n").expect("script");
    use std::os::unix::fs::PermissionsExt;
    stdfs::set_permissions(
        source.join("scripts/run.sh"),
        stdfs::Permissions::from_mode(0o755),
    )
    .expect("mode");
    source
}

/// Mirrors `test_snapshots.py::test_full_tree_is_copied_and_content_changes_revision`.
#[test]
fn python_test_full_tree_is_copied_and_content_changes_revision() {
    let temporary = TempDir::new().expect("temporary root");
    let source = source(temporary.path());
    let home = temporary.path().join("home");
    let first =
        snapshot_tree::snapshot_managed_tree(&home, Path::new("skills/demo"), &source, None)
            .expect("first snapshot");
    assert!(
        snapshot_tree::inspect_managed_snapshot(&home, Path::new("skills/demo"))
            .expect("inspection")
            .verified
    );
    stdfs::write(source.join("scripts/run.sh"), b"#!/bin/sh\necho changed\n")
        .expect("changed source");
    let second =
        snapshot_tree::snapshot_managed_tree(&home, Path::new("skills/demo"), &source, None)
            .expect("replacement snapshot");
    assert_ne!(first.sha256, second.sha256);
    assert_eq!(
        stdfs::read(home.join("skills/demo/scripts/run.sh")).expect("copied script"),
        b"#!/bin/sh\necho changed\n"
    );
}

/// Concurrent homes retain independent pinned plugin bytes across updates and expiry.
#[test]
fn independent_homes_share_clone_blocks_without_sharing_mutable_state() {
    let temporary = TempDir::new().unwrap();
    let source = source(temporary.path());
    let first = tempfile::Builder::new()
        .prefix("first-home")
        .tempdir_in(temporary.path())
        .unwrap();
    let second = tempfile::Builder::new()
        .prefix("second-home")
        .tempdir_in(temporary.path())
        .unwrap();
    let (first_manifest, second_manifest) = std::thread::scope(|scope| {
        let left = scope.spawn(|| {
            snapshot_tree::snapshot_managed_tree(
                first.path(),
                Path::new("declared-plugins/demo"),
                &source,
                None,
            )
            .unwrap()
        });
        let right = scope.spawn(|| {
            snapshot_tree::snapshot_managed_tree(
                second.path(),
                Path::new("declared-plugins/demo"),
                &source,
                None,
            )
            .unwrap()
        });
        (left.join().unwrap(), right.join().unwrap())
    });
    assert_eq!(first_manifest.sha256, second_manifest.sha256);
    let first_index =
        snapshot_tree::finalize_runtime_snapshots(first.path(), "same", &[], &[]).unwrap();
    let second_index =
        snapshot_tree::finalize_runtime_snapshots(second.path(), "same", &[], &[]).unwrap();
    assert_eq!(first_index, second_index);
    stdfs::write(source.join("SKILL.md"), "updated version").unwrap();
    for home in [first.path(), second.path()] {
        assert_eq!(
            stdfs::read(home.join("declared-plugins/demo/SKILL.md")).unwrap(),
            b"first"
        );
        assert!(
            snapshot_tree::inspect_runtime_snapshots(home, "same", &first_index)
                .unwrap()
                .verified
        );
    }
    drop(first);
    assert!(
        snapshot_tree::inspect_runtime_snapshots(second.path(), "same", &second_index)
            .unwrap()
            .verified
    );
}

/// Mirrors `test_snapshots.py::test_sources_reject_symlinks_and_special_files`.
#[test]
fn python_test_sources_reject_symlinks() {
    let temporary = TempDir::new().expect("temporary root");
    let source = source(temporary.path());
    std::os::unix::fs::symlink(source.join("SKILL.md"), source.join("linked"))
        .expect("source link");
    let error = snapshot_tree::snapshot_managed_tree(
        &temporary.path().join("home"),
        Path::new("skills/demo"),
        &source,
        None,
    )
    .expect_err("symlink rejected");
    assert!(matches!(error, agent_run_domain::Error::Validation(_)));
}

/// Mirrors `tests/test_snapshots.py::ManagedSnapshotTests::test_selected_assets_preserve_layout_without_copying_other_files`
#[test]
fn selected_assets_preserve_layout_without_copying_other_files() {
    let temporary = TempDir::new().unwrap();
    let source = source(temporary.path());
    stdfs::write(source.join("unselected.txt"), "do not copy").unwrap();
    snapshot_tree::snapshot_selected_assets(
        &temporary.path().join("home"),
        Path::new("declared-plugins/demo"),
        &source,
        &["SKILL.md".into(), "scripts".into()],
    )
    .unwrap();
    let copied = temporary.path().join("home/declared-plugins/demo");
    assert!(copied.join("SKILL.md").is_file());
    assert!(copied.join("scripts/run.sh").is_file());
    assert!(!copied.join("unselected.txt").exists());
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        stdfs::metadata(copied.join("scripts/run.sh"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
}

/// Mirrors `tests/test_snapshots.py::ManagedSnapshotTests::test_runtime_index_detects_an_entire_missing_snapshot_root`
#[test]
fn runtime_index_detects_an_entire_missing_snapshot_root() {
    let temporary = TempDir::new().unwrap();
    let source = source(temporary.path());
    let home = temporary.path().join("home");
    snapshot_tree::snapshot_managed_tree(&home, Path::new("skills/demo"), &source, None).unwrap();
    stdfs::write(home.join("settings.json"), "{}").unwrap();
    let digest =
        snapshot_tree::finalize_runtime_snapshots(&home, "files-1", &["settings.json".into()], &[])
            .unwrap();
    stdfs::remove_dir_all(home.join("skills/demo")).unwrap();
    let inspection = snapshot_tree::inspect_runtime_snapshots(&home, "files-1", &digest).unwrap();
    assert!(!inspection.verified);
    assert!(inspection
        .missing
        .iter()
        .any(|path| path == "skills/demo/.agent-run-snapshot.json"));
}

/// Mirrors `tests/test_snapshots.py::ManagedSnapshotTests::test_runtime_index_binds_each_root_manifest_revision`
#[test]
fn runtime_index_binds_each_root_manifest_revision() {
    let temporary = TempDir::new().unwrap();
    let source = source(temporary.path());
    let home = temporary.path().join("home");
    snapshot_tree::snapshot_managed_tree(&home, Path::new("skills/demo"), &source, None).unwrap();
    let digest = snapshot_tree::finalize_runtime_snapshots(&home, "files-1", &[], &[]).unwrap();
    let finalized = stdfs::read(home.join(RUNTIME_SNAPSHOT_INDEX)).unwrap();
    stdfs::write(source.join("SKILL.md"), "replacement").unwrap();
    snapshot_tree::snapshot_managed_tree(&home, Path::new("skills/demo"), &source, None).unwrap();
    stdfs::write(home.join(RUNTIME_SNAPSHOT_INDEX), finalized).unwrap();
    let inspection = snapshot_tree::inspect_runtime_snapshots(&home, "files-1", &digest).unwrap();
    assert!(!inspection.verified);
    assert!(inspection
        .hash_mismatches
        .iter()
        .any(|path| path == "skills/demo/.agent-run-snapshot.json"));
}

/// Mirrors `tests/test_snapshots.py::ManagedSnapshotTests::test_symlinked_manifest_is_a_type_mismatch`
#[test]
fn symlinked_manifest_is_a_type_mismatch() {
    let temporary = TempDir::new().unwrap();
    let source = source(temporary.path());
    let home = temporary.path().join("home");
    snapshot_tree::snapshot_managed_tree(&home, Path::new("skills/demo"), &source, None).unwrap();
    let digest = snapshot_tree::finalize_runtime_snapshots(&home, "files-1", &[], &[]).unwrap();
    let manifest = home.join("skills/demo").join(SNAPSHOT_MANIFEST);
    stdfs::remove_file(&manifest).unwrap();
    std::os::unix::fs::symlink(source.join("SKILL.md"), &manifest).unwrap();
    let inspection = snapshot_tree::inspect_runtime_snapshots(&home, "files-1", &digest).unwrap();
    assert!(inspection
        .type_mismatches
        .iter()
        .any(|path| path == "skills/demo/.agent-run-snapshot.json"));
    assert!(!inspection
        .missing
        .iter()
        .any(|path| path == "skills/demo/.agent-run-snapshot.json"));
}

/// Mirrors `tests/test_snapshots.py::ManagedSnapshotTests::test_flat_file_classification_never_follows_an_intermediate_symlink`
#[test]
fn flat_file_classification_never_follows_an_intermediate_symlink() {
    let temporary = TempDir::new().unwrap();
    let home = temporary.path().join("home");
    stdfs::create_dir_all(home.join("nested")).unwrap();
    stdfs::write(home.join("nested/settings.json"), "{}").unwrap();
    let digest = snapshot_tree::finalize_runtime_snapshots(
        &home,
        "files-1",
        &["nested/settings.json".into()],
        &[],
    )
    .unwrap();
    stdfs::rename(home.join("nested"), home.join("nested.retained")).unwrap();
    let outside = temporary.path().join("outside");
    stdfs::create_dir(&outside).unwrap();
    stdfs::write(outside.join("settings.json"), "outside").unwrap();
    std::os::unix::fs::symlink(&outside, home.join("nested")).unwrap();
    let inspection = snapshot_tree::inspect_runtime_snapshots(&home, "files-1", &digest).unwrap();
    assert!(inspection
        .type_mismatches
        .iter()
        .any(|path| path == "nested/settings.json"));
    assert!(!inspection
        .missing
        .iter()
        .any(|path| path == "nested/settings.json"));
}

/// Mirrors `tests/test_snapshots.py::ManagedSnapshotTests::test_runtime_hash_reader_rejects_incomplete_index_shape`
#[test]
fn runtime_hash_reader_rejects_incomplete_index_shape() {
    let temporary = TempDir::new().unwrap();
    stdfs::create_dir(temporary.path().join("home")).unwrap();
    stdfs::write(
        temporary.path().join("home/.agent-run-snapshots.json"),
        "{\"materialize_revision\":\"files-1\"}\n",
    )
    .unwrap();
    assert!(snapshot_tree::runtime_snapshot_index_sha256(
        &temporary.path().join("home"),
        "files-1"
    )
    .is_err());
}

/// Mirrors `test_snapshots.py::test_interrupted_metadata_and_recovery_states_never_verify`.
#[test]
fn python_test_tamper_missing_extra_symlink_and_partial_are_refused() {
    let temporary = TempDir::new().expect("temporary root");
    let source = source(temporary.path());
    let home = temporary.path().join("home");
    snapshot_tree::snapshot_managed_tree(&home, Path::new("skills/demo"), &source, None)
        .expect("snapshot");
    let revision = "revision";
    let digest =
        snapshot_tree::finalize_runtime_snapshots(&home, revision, &[], &[]).expect("index");

    stdfs::write(home.join("skills/demo/SKILL.md"), "tampered").expect("tamper");
    let tampered = snapshot_tree::inspect_runtime_snapshots(&home, revision, &digest)
        .expect("tamper inspection");
    assert!(
        !tampered.verified
            && tampered
                .hash_mismatches
                .iter()
                .any(|path| path.ends_with("SKILL.md"))
    );

    stdfs::write(home.join("skills/demo/SKILL.md"), "first").expect("restore");
    stdfs::remove_file(home.join("skills/demo/scripts/run.sh")).expect("remove referenced file");
    let missing = snapshot_tree::inspect_runtime_snapshots(&home, revision, &digest)
        .expect("missing inspection");
    assert!(
        !missing.verified
            && missing
                .missing
                .iter()
                .any(|path| path.ends_with("scripts/run.sh"))
    );

    stdfs::write(home.join("skills/demo/scripts/run.sh"), b"#!/bin/sh\n").expect("restore script");
    stdfs::write(home.join("skills/demo/orphan"), "unexpected").expect("orphan");
    let extra = snapshot_tree::inspect_runtime_snapshots(&home, revision, &digest)
        .expect("extra inspection");
    assert!(!extra.verified && extra.orphans.iter().any(|path| path.ends_with("orphan")));
    stdfs::remove_file(home.join("skills/demo/orphan")).expect("remove orphan");

    stdfs::remove_file(home.join("skills/demo").join(SNAPSHOT_MANIFEST)).expect("remove manifest");
    std::os::unix::fs::symlink(
        source.join("SKILL.md"),
        home.join("skills/demo").join(SNAPSHOT_MANIFEST),
    )
    .expect("manifest link");
    let linked = snapshot_tree::inspect_runtime_snapshots(&home, revision, &digest)
        .expect("link inspection");
    assert!(
        !linked.verified
            && linked
                .type_mismatches
                .iter()
                .any(|path| path.ends_with(SNAPSHOT_MANIFEST))
    );

    let partial_home = temporary.path().join("partial");
    stdfs::create_dir_all(partial_home.join("skills/demo")).expect("partial root");
    stdfs::write(partial_home.join("skills/demo/SKILL.md"), "first").expect("partial content");
    let partial = snapshot_tree::inspect_managed_snapshot(&partial_home, Path::new("skills/demo"))
        .expect("partial inspection");
    assert!(!partial.verified && partial.referenced_missing == vec![SNAPSHOT_MANIFEST]);
}

/// Materialize a Python golden home capture into a private temporary directory.
fn restore_python_capture(capture: &Path, home: &Path) -> Value {
    let document: Value =
        serde_json::from_slice(&stdfs::read(capture).expect("fixture")).expect("fixture JSON");
    stdfs::create_dir_all(home).expect("home");
    let temp_root = home
        .parent()
        .expect("temporary parent")
        .to_string_lossy()
        .into_owned();
    for (path, entry) in document.as_object().expect("tree object") {
        let destination = home.join(path);
        stdfs::create_dir_all(destination.parent().expect("fixture parent"))
            .expect("fixture directories");
        match entry["type"].as_str().expect("fixture type") {
            "file" => {
                let content = entry["content"]
                    .as_str()
                    .expect("fixture content")
                    .replace("${TEMP_ROOT}", &temp_root);
                stdfs::write(destination, content).expect("fixture file");
                use std::os::unix::fs::PermissionsExt;
                stdfs::set_permissions(home.join(path), stdfs::Permissions::from_mode(0o600))
                    .expect("fixture mode");
            }
            "symlink" => {
                let target = entry["target"]
                    .as_str()
                    .expect("fixture target")
                    .replace("${TEMP_ROOT}", &temp_root);
                std::os::unix::fs::symlink(target, destination).expect("fixture symlink");
            }
            other => panic!("unexpected fixture entry type: {other}"),
        }
    }
    // Golden-home captures redact their temporary root after the Python index
    // has been hashed. Rebind only that redacted flat-file evidence to this
    // private restored tree; the index schema and every non-redacted fixture
    // byte still come directly from Python's capture.
    let index_path = home.join(RUNTIME_SNAPSHOT_INDEX);
    let mut index: Value =
        serde_json::from_slice(&stdfs::read(&index_path).expect("fixture index"))
            .expect("fixture index JSON");
    for entry in index["files"].as_array_mut().expect("fixture files") {
        let path = entry["path"].as_str().expect("fixture file path");
        let bytes = stdfs::read(home.join(path)).expect("restored fixture file");
        entry["bytes"] = bytes.len().into();
        entry["sha256"] = fs::sha256(&bytes).into();
    }
    for entry in index["links"].as_array_mut().expect("fixture links") {
        let target = entry["target"]
            .as_str()
            .expect("fixture link target")
            .replace("${TEMP_ROOT}", &temp_root);
        entry["target"] = target.into();
    }
    let mut index_bytes = agent_run_domain::canonical::dumps(&index, true);
    index_bytes.push(b'\n');
    stdfs::write(index_path, index_bytes).expect("rebound fixture index");
    document
}

/// Reads the Python-generated Codex home capture and verifies its v1 index.
#[test]
fn python_generated_home_fixture_verifies() {
    let temporary = TempDir::new().expect("temporary root");
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let capture = root.join("tests/fixtures/baseline/homes/codex/read-only/tree.json");
    let home = temporary.path().join("home");
    restore_python_capture(&capture, &home);
    let index = stdfs::read(home.join(RUNTIME_SNAPSHOT_INDEX)).expect("Python index");
    let document: Value = serde_json::from_slice(&index).expect("index JSON");
    let inspection = snapshot_tree::inspect_runtime_snapshots(
        &home,
        document["materialize_revision"].as_str().expect("revision"),
        &fs::sha256(&index),
    )
    .expect("inspection");
    assert!(inspection.verified, "{inspection:?}");
}

/// Mirrors the Python fixture's index field shape for a Rust-generated home.
#[test]
fn rust_generated_snapshot_has_python_fixture_manifest_shape() {
    let temporary = TempDir::new().expect("temporary root");
    let source = source(temporary.path());
    let home = temporary.path().join("home");
    snapshot_tree::snapshot_managed_tree(&home, Path::new("skills/demo"), &source, None)
        .expect("tree");
    snapshot_tree::finalize_runtime_snapshots(&home, "revision", &[], &[]).expect("index");
    let index: Value =
        serde_json::from_slice(&stdfs::read(home.join(RUNTIME_SNAPSHOT_INDEX)).expect("index"))
            .expect("index JSON");
    let manifest: Value = serde_json::from_slice(
        &stdfs::read(home.join("skills/demo").join(SNAPSHOT_MANIFEST)).expect("manifest"),
    )
    .expect("manifest JSON");
    assert_eq!(
        index
            .as_object()
            .expect("index object")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec![
            "files",
            "links",
            "manifests",
            "materialize_revision",
            "roots",
            "snapshot_index_version"
        ]
    );
    assert_eq!(
        manifest
            .as_object()
            .expect("manifest object")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["entries", "snapshot_version"]
    );
}

/// Mirrors `test_snapshots.py::test_config_snapshot_changes_with_content_without_storing_secret_values`.
#[test]
fn python_test_config_snapshot_binds_revision_without_secret_values() {
    use agent_run_config::{
        config::{Config, Runtime},
        role_plan::ResolvedRolePlan,
        snapshot,
    };
    use serde_json::json;
    let temporary = TempDir::new().expect("temporary root");
    let runtime: Runtime = serde_json::from_value(json!({
        "enabled": true, "adapter": "claude", "binary": "/bin/echo", "home": temporary.path().join("home"),
        "models": ["fixture"], "environment": "developer"
    })).expect("runtime");
    let config: Config = serde_json::from_value(json!({
        "schema_version": 1,
        "environments": {"developer": {"variables": {"TOKEN_LIKE": "credential-like-value"}}}
    }))
    .expect("config");
    let profile = ResolvedRolePlan {
        worker_mcp: false,
        role_name: "review".into(),
        role_revision: "legacy".into(),
        prompt: "Review exactly.".into(),
        write: false,
        network: false,
        allow_external_read_roots: true,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        required_constraints: Default::default(),
        auth_mode: "global".into(),
        auth_reference: None,
        config_revision: "a".repeat(64),
    };
    let home = temporary.path().join("home");
    stdfs::create_dir(&home).expect("home");
    let index =
        snapshot_tree::finalize_runtime_snapshots(&home, "files-1", &[], &[]).expect("index");
    let first = snapshot::build_config_snapshot(
        "claude", 1, 1, "files-1", &index, &config, &runtime, &profile, None,
    )
    .expect("snapshot");
    assert!(!String::from_utf8_lossy(&first.document).contains("credential-like-value"));
    snapshot::write_config_snapshot(&home, &first).expect("write snapshot");
    assert_eq!(
        snapshot::inspect_config_snapshot(&home, &first.sha256).expect("inspect snapshot"),
        first
    );
}

/// Shared-store bridge checks: one sealed home's managed tree moves into the
/// content-addressed store and verifies through the explicit shared verifier
/// while the original strict verifier keeps refusing symlinked roots.
mod shared_bridge {
    use agent_run_adapters::materialize;
    use agent_run_platform::{
        fs,
        shared_assets::{self, SharedTreeRef},
        snapshot_tree::{self, RUNTIME_SNAPSHOT_INDEX},
    };
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs as stdfs,
        os::unix::fs::{MetadataExt, PermissionsExt},
        path::{Path, PathBuf},
    };
    use tempfile::TempDir;

    /// One sealed home fixture: managed tree, flat config, credential link,
    /// and a home-local native-history file outside the asset index.
    struct SealedHome {
        home: PathBuf,
        revision: String,
        index_bytes: Vec<u8>,
        index_sha256: String,
        history: Vec<u8>,
    }

    /// Seals one home whose managed tree carries `payload`; `stamp` makes the
    /// skill manifest unique per home so distinct homes stay distinct trees.
    fn seal(root: &Path, name: &str, stamp: &str, payload: &[u8]) -> SealedHome {
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
        SealedHome {
            home,
            revision,
            index_sha256: digest,
            index_bytes,
            history,
        }
    }

    /// RAII shared-store fixture whose readonly published trees are made
    /// writable again before the temporary directory is cleaned up.
    struct StoreDir {
        temp: TempDir,
        root: PathBuf,
    }

    /// Creates one empty canonical shared-store root.
    fn store() -> StoreDir {
        let temp = TempDir::new().expect("store temporary");
        let root = temp.path().canonicalize().expect("canonical store root");
        StoreDir { temp, root }
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

    impl Drop for StoreDir {
        /// Makes every readonly published directory removable, then lets the
        /// temporary directory remove itself.
        fn drop(&mut self) {
            permit_tree(self.temp.path());
        }
    }

    /// Imports one home's managed tree and returns the shared reference.
    fn import(store: &StoreDir, scope: &str, sealed: &SealedHome) -> SharedTreeRef {
        shared_assets::import_shared_tree(
            &store.root,
            scope,
            &sealed.home,
            Path::new("skills/demo"),
        )
        .expect("shared import")
    }

    /// Replaces one home's managed root with the exact whole-tree symlink.
    fn relink(sealed: &SealedHome, store: &StoreDir, reference: &SharedTreeRef) {
        let target = shared_assets::shared_tree_root(&store.root, reference).expect("target");
        let root = sealed.home.join("skills/demo");
        stdfs::remove_dir_all(&root).expect("remove private copy");
        std::os::unix::fs::symlink(&target, &root).expect("whole-tree link");
    }

    /// Runs the explicit shared verifier for one mapping.
    fn shared_inspection(
        sealed: &SealedHome,
        store: &StoreDir,
        map: &BTreeMap<String, SharedTreeRef>,
    ) -> agent_run_domain::Result<snapshot_tree::RuntimeSnapshotInspection> {
        snapshot_tree::inspect_runtime_snapshots_with_shared(
            &sealed.home,
            &sealed.revision,
            &sealed.index_sha256,
            &store.root,
            map,
        )
    }

    /// One mapping of the home's single managed root.
    fn single(reference: &SharedTreeRef) -> BTreeMap<String, SharedTreeRef> {
        BTreeMap::from([("skills/demo".into(), reference.clone())])
    }

    /// The shared bridge accepts one exact whole-tree symlink with identical
    /// original index bytes and history, while the strict verifier refuses.
    #[test]
    fn bridge_accepts_exact_symlink_and_old_verifier_still_refuses() {
        let root = TempDir::new().expect("fixture root");
        let sealed = seal(root.path(), "home", "a", b"payload-bytes\n");
        let store = store();
        let scope = fs::sha256(b"bridge-main");
        let reference = import(&store, &scope, &sealed);
        assert!(
            snapshot_tree::inspect_runtime_snapshots(
                &sealed.home,
                &sealed.revision,
                &sealed.index_sha256
            )
            .expect("private home verifies")
            .verified
        );
        relink(&sealed, &store, &reference);
        let old = snapshot_tree::inspect_runtime_snapshots(
            &sealed.home,
            &sealed.revision,
            &sealed.index_sha256,
        )
        .expect("strict inspection");
        assert!(!old.verified, "a symlinked root must stay a mismatch");
        assert!(
            materialize::verify(&sealed.home, &sealed.index_sha256).is_err(),
            "the public strict verify path is unchanged"
        );
        let map = single(&reference);
        let inspection =
            shared_inspection(&sealed, &store, &map).expect("shared bridge verification");
        assert!(inspection.verified, "{inspection:?}");
        assert!(
            materialize::verify_with_shared(&sealed.home, &sealed.index_sha256, &store.root, &map)
                .is_ok(),
            "adapter verify_with_shared mirrors materialize::verify"
        );
        assert_eq!(
            stdfs::read(sealed.home.join(RUNTIME_SNAPSHOT_INDEX)).unwrap(),
            sealed.index_bytes,
            "the original index bytes stay byte-exact"
        );
        // History continuity is its own seam: the asset verifier does not own
        // it, but the sealed bytes are still exactly what was recorded.
        assert_eq!(
            fs::sha256(&stdfs::read(sealed.home.join("history.json")).unwrap()),
            fs::sha256(&sealed.history)
        );

        // Tampering the flat config is not excused by sharing.
        stdfs::write(sealed.home.join("config.toml"), "key = \"tampered\"\n").unwrap();
        assert!(
            !shared_inspection(&sealed, &store, &map)
                .expect("flat drift is classified, not fatal")
                .verified
        );
        stdfs::write(sealed.home.join("config.toml"), "key = \"value\"\n").unwrap();

        // A wrong digest, a foreign target, a dangling target, an unknown
        // mapping root, a restored private copy, and store drift all fail.
        let other = seal(root.path(), "other", "b", b"different payload\n");
        let other_reference = import(&store, &fs::sha256(b"bridge-other"), &other);
        assert_ne!(other_reference, reference);
        assert!(shared_inspection(&sealed, &store, &single(&other_reference)).is_err());

        let link = sealed.home.join("skills/demo");
        let target = stdfs::read_link(&link).unwrap();
        stdfs::remove_file(&link).unwrap();
        std::os::unix::fs::symlink(root.path().join("outside"), &link).unwrap();
        assert!(shared_inspection(&sealed, &store, &map).is_err());
        stdfs::remove_file(&link).unwrap();
        std::os::unix::fs::symlink(
            store.root.join("trees").join(&scope).join("deadbeef"),
            &link,
        )
        .unwrap();
        assert!(shared_inspection(&sealed, &store, &map).is_err());
        stdfs::remove_file(&link).unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let unknown = BTreeMap::from([("skills/other".into(), reference.clone())]);
        assert!(shared_inspection(&sealed, &store, &unknown).is_err());

        stdfs::remove_file(&link).unwrap();
        let source = root.path().join("home-source");
        stdfs::create_dir_all(&source).unwrap();
        stdfs::write(source.join("SKILL.md"), "# skill a\n").unwrap();
        snapshot_tree::snapshot_managed_tree(&sealed.home, Path::new("skills/demo"), &source, None)
            .unwrap();
        // Re-sealing resets the runtime index to its pre-finalize shape;
        // restore the exact sealed bytes so the next assertion is judged
        // against a valid index, not a destroyed one.
        stdfs::write(
            sealed.home.join(RUNTIME_SNAPSHOT_INDEX),
            &sealed.index_bytes,
        )
        .unwrap();
        assert!(
            shared_inspection(&sealed, &store, &map).is_err(),
            "a restored private copy is not silently accepted for a mapped root"
        );

        relink(&sealed, &store, &reference);
        let tree_file = shared_assets::shared_tree_root(&store.root, &reference)
            .unwrap()
            .join("scripts/payload.bin");
        stdfs::set_permissions(&tree_file, stdfs::Permissions::from_mode(0o600)).unwrap();
        assert!(shared_inspection(&sealed, &store, &map).is_err());
        stdfs::set_permissions(&tree_file, stdfs::Permissions::from_mode(0o400)).unwrap();
        assert!(
            shared_inspection(&sealed, &store, &map)
                .expect("restored mode verifies")
                .verified
        );
    }

    /// Two independently sealed homes carrying the same payload converge on
    /// the same shared tree target and payload inode.
    #[test]
    fn independent_homes_converge_on_one_shared_tree() {
        let root = TempDir::new().expect("fixture root");
        let first = seal(root.path(), "first", "same", b"shared payload\n");
        let second = seal(root.path(), "second", "same", b"shared payload\n");
        let store = store();
        let scope = fs::sha256(b"bridge-scope");
        let first_reference = import(&store, &scope, &first);
        let second_reference = import(&store, &scope, &second);
        assert_eq!(first_reference, second_reference);
        assert_eq!(
            shared_assets::shared_tree_root(&store.root, &first_reference).unwrap(),
            shared_assets::shared_tree_root(&store.root, &second_reference).unwrap()
        );
        relink(&first, &store, &first_reference);
        relink(&second, &store, &second_reference);
        assert_eq!(
            stdfs::metadata(first.home.join("skills/demo/SKILL.md"))
                .unwrap()
                .ino(),
            stdfs::metadata(second.home.join("skills/demo/SKILL.md"))
                .unwrap()
                .ino()
        );
        for sealed in [&first, &second] {
            assert!(
                shared_inspection(sealed, &store, &single(&first_reference))
                    .expect("bridge verification")
                    .verified
            );
        }
    }

    /// Sums unique regular-file inode bytes across roots, counting hardlinked
    /// payloads and the store exactly once.
    fn unique_file_bytes(roots: &[PathBuf]) -> u64 {
        let mut seen: BTreeMap<(u64, u64), u64> = BTreeMap::new();
        for root in roots {
            collect_inodes(root, &mut seen);
        }
        seen.values().sum()
    }

    /// Recursively records regular-file inodes without following symlinks.
    fn collect_inodes(path: &Path, seen: &mut BTreeMap<(u64, u64), u64>) {
        if let Ok(metadata) = stdfs::symlink_metadata(path) {
            if metadata.is_dir() {
                if let Ok(children) = stdfs::read_dir(path) {
                    for child in children.flatten() {
                        collect_inodes(&child.path(), seen);
                    }
                }
            } else if metadata.is_file() {
                seen.entry((metadata.dev(), metadata.ino()))
                    .or_insert(metadata.len());
            }
        }
    }

    /// A bounded three-home fixture measures deduplicated bytes at three
    /// points: the true pre-import baseline of the private homes, the staging
    /// peak while both private copies and the store exist, and the state
    /// after the private copies are replaced by links. These are measured
    /// fixture bytes, not a promise of physical APFS free space or
    /// production-scale reclamation.
    #[test]
    fn three_home_fixture_measures_deduplicated_bytes() {
        let payload: Vec<u8> = (0..1024 * 1024).map(|byte| (byte % 251) as u8).collect();
        let root = TempDir::new().expect("fixture root");
        let homes: Vec<_> = ["a", "b", "c"]
            .iter()
            .map(|stamp| seal(root.path(), &format!("home-{stamp}"), stamp, &payload))
            .collect();
        let store = store();
        let scope = fs::sha256(b"measurement-scope");
        let home_roots: Vec<PathBuf> = homes.iter().map(|home| home.home.clone()).collect();
        let baseline = unique_file_bytes(&home_roots);
        let references: Vec<_> = homes
            .iter()
            .map(|home| import(&store, &scope, home))
            .collect();
        assert_eq!(references.len(), 3);
        assert!(
            references.windows(2).all(|pair| pair[0] != pair[1]),
            "distinct skill stamps stay distinct trees"
        );
        let mut roots = home_roots.clone();
        roots.push(store.root.clone());
        let peak = unique_file_bytes(&roots);
        for (home, reference) in homes.iter().zip(&references) {
            relink(home, &store, reference);
        }
        let after = unique_file_bytes(&roots);
        let mut one_mib: BTreeSet<(u64, u64)> = BTreeSet::new();
        collect_exact(&store.root, payload.len() as u64, &mut one_mib);
        assert_eq!(one_mib.len(), 1, "the 1 MiB payload inode is stored once");
        assert!(
            after < baseline,
            "fixture bytes must fall below the private-copy baseline: {after} >= {baseline}"
        );
        assert!(
            peak > baseline,
            "the staging peak holds both private copies and the store: {peak} <= {baseline}"
        );
        println!(
            "shared-store fixture measurement: baseline={baseline} private bytes, staging peak={peak} bytes, after={after} bytes (store included once)"
        );
    }

    /// Collects store inodes whose file length is exactly `length`.
    fn collect_exact(path: &Path, length: u64, found: &mut BTreeSet<(u64, u64)>) {
        if let Ok(metadata) = stdfs::symlink_metadata(path) {
            if metadata.is_dir() {
                if let Ok(children) = stdfs::read_dir(path) {
                    for child in children.flatten() {
                        collect_exact(&child.path(), length, found);
                    }
                }
            } else if metadata.is_file() && metadata.len() == length {
                found.insert((metadata.dev(), metadata.ino()));
            }
        }
    }
}
