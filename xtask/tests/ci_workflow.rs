//! Static regression coverage for the Rust-only CI and release workflows.

use std::{fs, path::Path};

/// Reads a repository file relative to the workspace root.
fn repository_file(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join(path))
        .unwrap_or_else(|error| panic!("cannot read {path}: {error}"))
}

/// Ensures primary CI rejects legacy sources and runs every Rust release gate.
#[test]
fn ci_is_rust_only_and_checks_the_desktop_transport() {
    let workflow = repository_file(".github/workflows/ci.yml");
    for required in [
        "test -z \"$(git ls-files '*.py')\"",
        "cargo xtask check",
        "cargo xtask qualify --release",
        "cargo xtask evidence verify",
        "cargo xtask archive --verify",
        "cargo xtask release build-native",
        "cargo xtask release verify",
        "cargo build --locked --release --package agent-run --bin agent-run",
        "node --test scripts/check-desktop-transport.cjs",
        "validation-only-linux",
    ] {
        assert!(workflow.contains(required), "CI is missing {required:?}");
    }
    for forbidden in ["setup-python", "pip install", "pytest", "uv sync"] {
        assert!(
            !workflow.contains(forbidden),
            "CI still contains legacy command {forbidden:?}"
        );
    }
}

/// Ensures tagged releases publish only verified native and source archives.
#[test]
fn release_publishes_checksummed_native_assets() {
    let workflow = repository_file(".github/workflows/release.yml");
    for required in [
        "git cat-file -t",
        "Cargo.toml",
        "cargo xtask check",
        "cargo xtask qualify --release",
        "cargo xtask evidence verify",
        "cargo xtask release build-native",
        "cargo xtask release verify",
        "cargo xtask archive --revision HEAD",
        "aarch64-apple-darwin.tar.gz",
        "SHA256SUMS",
        "subject-checksums: dist/SHA256SUMS",
        "gh release create",
    ] {
        assert!(
            workflow.contains(required),
            "release workflow is missing {required:?}"
        );
    }
    for forbidden in [
        "setup-python",
        "pip install",
        "pytest",
        "uv sync",
        ".whl",
        "sdist",
    ] {
        assert!(
            !workflow.contains(forbidden),
            "release still contains legacy artifact {forbidden:?}"
        );
    }
}
