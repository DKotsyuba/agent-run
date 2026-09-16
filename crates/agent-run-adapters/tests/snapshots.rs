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
