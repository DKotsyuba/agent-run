//! Source archive round-trip coverage for the M56 handoff tool.

use std::{fs, path::Path};
use tempfile::tempdir;
use xtask::archive;

/// Builds and verifies the source archive from the checked-out revision.
#[test]
fn source_archive_round_trip_records_lock_and_excludes_caches() {
    // Rust-only tooling has no Python counterpart and therefore no Mirrors citation.
    let temporary = tempdir().expect("temporary output");
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root");
    let output = temporary.path().join("source.tar");
    archive::build(root, &output, "HEAD").expect("archive");
    archive::verify(root, &output).expect("archive verification");

    let listing = std::process::Command::new("tar")
        .args(["-tf", output.to_str().expect("UTF-8 archive path")])
        .output()
        .expect("tar listing");
    assert!(listing.status.success(), "tar listing failed");
    let listing = String::from_utf8(listing.stdout).expect("UTF-8 tar listing");
    assert!(listing.contains("ARCHIVE-MANIFEST.json"));
    assert!(listing.contains("Cargo.lock"));
    assert!(!listing.contains("/target/"));
    assert!(!listing.contains("/.cargo-home/"));
    assert!(!listing.contains("/.wt/"));
    assert!(!fs::metadata(&output).expect("archive output").is_dir());
}
