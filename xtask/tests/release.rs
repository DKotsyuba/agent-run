//! Sealed release manifest gate and pointer-switch safety.
//!
//! Mirrors the sealed-release contract in
//! `tests/test_release_script.py::LocalTests`. Python's `local.switch` /
//! `local.verify_complete` operate on a legacy venv-based layout
//! (`venv/bin/python`, `venv/bin/agent-run`, a site-packages file) behind a
//! `./`-prefixed manifest that also tolerates an external Python
//! interpreter symlink; Rust's sealed release (xtask/src/release.rs) is a
//! single static binary with no venv/python concept at all, so these tests
//! cover the safety invariants that do carry over -- an incomplete or
//! tampered release can never become `current`, and a manifest can never
//! list the same file twice -- against the real (`bin/agent-run`,
//! `metadata.json`) layout instead of reconstructing the legacy one.

use std::fs;
use tempfile::tempdir;
use xtask::{deploy, release};

/// Mirrors `tests/test_release_script.py::LocalTests::test_complete_and_manifest_gate`.
#[test]
fn python_release_incomplete_or_corrupt_never_becomes_current() {
    let temporary = tempdir().expect("temporary prefix");
    let prefix = temporary.path().join("standalone");
    let home = temporary.path().join("home");
    fs::create_dir_all(&home).expect("temporary home");

    let good_binary = temporary.path().join("good");
    fs::write(&good_binary, "good binary").expect("fixture binary");
    let good = release::build(&prefix, "1.0.0", &good_binary).expect("sealed release");
    deploy::deploy(&prefix, &home, &good, false).expect("baseline install");
    let baseline_current = fs::read_link(prefix.join("current")).expect("current pointer");

    // Missing COMPLETE marker: verify (and therefore deploy) refuses it, and
    // `current` stays on the last good release.
    let bad_binary = temporary.path().join("bad");
    fs::write(&bad_binary, "bad binary").expect("fixture binary");
    let bad = release::build(&prefix, "2.0.0", &bad_binary).expect("sealed release");
    fs::remove_file(bad.join("COMPLETE")).expect("strip completion marker");
    assert!(
        release::verify(&bad).is_err(),
        "incomplete release must fail verification"
    );
    assert!(
        deploy::deploy(&prefix, &home, &bad, false).is_err(),
        "deploy must refuse an incomplete release"
    );
    assert_eq!(
        fs::read_link(prefix.join("current")).expect("current pointer"),
        baseline_current,
        "a refused deploy must never move the current pointer"
    );

    // Restore COMPLETE, then tamper with the sealed binary instead.
    fs::write(bad.join("COMPLETE"), "complete\n").expect("reseal completion marker");
    fs::write(bad.join("bin/agent-run"), "corrupted").expect("tamper with sealed binary");
    let error = release::verify(&bad).expect_err("corrupt release must fail verification");
    assert!(
        error.to_lowercase().contains("corrupt"),
        "error should name corruption: {error}"
    );
    assert!(
        deploy::deploy(&prefix, &home, &bad, false).is_err(),
        "deploy must refuse a tampered release"
    );
    assert_eq!(
        fs::read_link(prefix.join("current")).expect("current pointer"),
        baseline_current,
        "a refused deploy must never move the current pointer"
    );
}

/// Mirrors `tests/test_release_script.py::LocalTests::test_legacy_seal_normalizes_paths_and_allows_external_python_symlink`
/// (the manifest-duplicate assertion only; see the module doc comment for
/// the parts of that Python test that do not apply here).
#[test]
fn python_release_manifest_rejects_duplicate_entries() {
    let temporary = tempdir().expect("temporary prefix");
    let binary = temporary.path().join("agent-run");
    fs::write(&binary, "native binary").expect("fixture binary");
    let release = release::build(temporary.path(), "1.0.0", &binary).expect("sealed release");
    release::verify(&release).expect("freshly sealed release is valid");

    let manifest = fs::read_to_string(release.join("SHA256SUMS")).expect("read manifest");
    let duplicated_line = manifest
        .lines()
        .find(|line| line.ends_with("bin/agent-run"))
        .expect("manifest lists the sealed binary")
        .to_owned();
    let manifest = format!("{manifest}{duplicated_line}\n");
    fs::write(release.join("SHA256SUMS"), manifest).expect("tamper with manifest");

    let error = release::verify(&release).expect_err("duplicate manifest entries must be rejected");
    assert!(
        error.contains("duplicate"),
        "error should name the duplicate: {error}"
    );
}
