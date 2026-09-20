//! Static regression coverage for the Rust-only CI and release workflows.

use std::{fs, path::Path};

/// Reads a repository file relative to the workspace root.
fn repository_file(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join(path))
        .unwrap_or_else(|error| panic!("cannot read {path}: {error}"))
}

/// Ensures primary CI runs locked gates and sealed builds on both native release targets.
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
        "- os: macos-15\n            label: supported-macos-arm64\n            target: aarch64-apple-darwin\n            architecture: arm64",
        "- os: ubuntu-latest\n            label: configured-release-target-linux-x86_64-pending-hosted-evidence\n            target: x86_64-unknown-linux-gnu\n            architecture: x86_64",
        "test \"$(uname -m)\" = \"${{ matrix.architecture }}\"",
        "node --test scripts/check-desktop-transport.cjs",
        "configured-release-target-linux-x86_64-pending-hosted-evidence",
    ] {
        assert!(workflow.contains(required), "CI is missing {required:?}");
    }
    for forbidden in ["setup-python", "pip install", "pytest", "uv sync"] {
        assert!(
            !workflow.contains(forbidden),
            "CI still contains legacy command {forbidden:?}"
        );
    }
    assert!(
        !workflow.contains("macos-latest"),
        "CI must pin the arm64 runner"
    );
    let (matrix_job, release_contract) = workflow
        .split_once("\n  release-contract:")
        .expect("CI must retain its release-contract job");
    for (name, job) in [
        ("matrix", matrix_job),
        ("release-contract", release_contract),
    ] {
        let fetch = job
            .find("cargo fetch --locked")
            .unwrap_or_else(|| panic!("{name} job must fetch the locked graph"));
        let gate = job
            .find("cargo xtask")
            .unwrap_or_else(|| panic!("{name} job must run an xtask gate"));
        assert!(
            fetch < gate,
            "{name} job must fetch before offline xtask use"
        );
    }
}

/// Ensures tagged releases aggregate two verified native archives and one source archive.
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
        "- os: macos-15\n            target: aarch64-apple-darwin\n            architecture: arm64",
        "- os: ubuntu-latest\n            target: x86_64-unknown-linux-gnu\n            architecture: x86_64",
        "agent-run-$version-${{ matrix.target }}.tar.gz",
        "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02",
        "actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
        "merge-multiple: true",
        "needs: [gate, native]",
        "SHA256SUMS",
        "subject-checksums: dist/SHA256SUMS",
        "GH_REPO: ${{ github.repository }}",
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
    assert_eq!(
        workflow
            .matches("cargo xtask archive --revision HEAD")
            .count(),
        1,
        "the verified source archive must be built once"
    );
    assert_eq!(
        workflow.matches("gh release create").count(),
        1,
        "all archives must be published by one GitHub Release operation"
    );
    assert!(
        !workflow.contains("macos-latest"),
        "release builds must pin the arm64 runner"
    );
    let (header, jobs) = workflow
        .split_once("\njobs:")
        .expect("release workflow must declare jobs");
    let (gate, jobs) = jobs
        .split_once("\n  native:")
        .expect("release workflow must declare a native job");
    let (native, publish) = jobs
        .split_once("\n  publish:")
        .expect("release workflow must declare a publish job");
    assert!(
        header.contains("permissions:\n  contents: read"),
        "workflow-level permissions must be read-only"
    );
    assert!(
        !header.contains("write")
            && !gate.contains("contents: write")
            && !native.contains("contents: write"),
        "write permissions must not be available before publish"
    );
    for permission in [
        "contents: write",
        "id-token: write",
        "attestations: write",
        "artifact-metadata: write",
    ] {
        assert!(
            publish.contains(permission),
            "publish job is missing {permission:?}"
        );
    }
    assert!(
        publish.contains("needs: [gate, native]"),
        "publish must wait for both verified artifact jobs"
    );
    for (name, job) in [("gate", gate), ("native", native)] {
        let fetch = job
            .find("cargo fetch --locked")
            .unwrap_or_else(|| panic!("{name} job must fetch the locked graph"));
        let gate = job
            .find("cargo xtask")
            .unwrap_or_else(|| panic!("{name} job must run an xtask gate"));
        assert!(
            fetch < gate,
            "{name} job must fetch before offline xtask use"
        );
    }
    assert!(
        native.contains("run: cargo xtask check"),
        "both native matrix entries must run the full locked workspace gates"
    );
}

/// Ensures dependency automation covers both workflow actions and locked Rust crates weekly.
#[test]
fn dependabot_checks_actions_and_cargo_weekly() {
    let configuration = repository_file(".github/dependabot.yml");
    for required in [
        "package-ecosystem: github-actions",
        "package-ecosystem: cargo",
        "interval: weekly",
    ] {
        assert!(
            configuration.contains(required),
            "Dependabot is missing {required:?}"
        );
    }
}

/// Pins the xtask alias, local checks, and native release to the reviewed lockfile.
#[test]
fn xtask_entrypoints_and_builds_use_locked_resolution() {
    let alias = repository_file(".cargo/config.toml");
    assert!(
        alias.contains("xtask = \"run --locked --package xtask --\""),
        "the xtask alias must use the committed lockfile"
    );
    let source = repository_file("xtask/src/main.rs");
    assert!(
        source.contains("\"clippy\",\n            \"--offline\",\n            \"--locked\","),
        "cargo xtask check must lock clippy resolution"
    );
    assert!(
        source.contains("\"--\",\n            \"-D\",\n            \"warnings\","),
        "cargo xtask check must deny clippy warnings"
    );
    assert!(
        source.contains("\"test\",\n            \"--offline\",\n            \"--locked\","),
        "cargo xtask check must lock test resolution"
    );
    assert!(
        source.contains(
            "\"build\",\n        \"--offline\",\n        \"--locked\",\n        \"--release\","
        ),
        "native release builds must use the committed lockfile"
    );
}
