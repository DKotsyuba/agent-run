//! Static regression coverage for the native CI and release workflows.

use std::{fs, path::Path};

/// Reads a repository file relative to the workspace root.
fn repository_file(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join(path))
        .unwrap_or_else(|error| panic!("cannot read {path}: {error}"))
}

/// Ensures primary CI gates macOS releases while retaining visible Linux validation.
#[test]
fn ci_checks_native_release_and_desktop_transport() {
    let workflow = repository_file(".github/workflows/ci.yml");
    for required in [
        "cargo xtask check",
        "cargo xtask archive --verify",
        "cargo xtask release build-native",
        "cargo xtask release verify",
        "- os: macos-15\n            label: supported-macos-arm64\n            target: aarch64-apple-darwin\n            architecture: arm64",
        "- os: ubuntu-latest\n            label: validation-only-linux-x86_64-unqualified\n            target: x86_64-unknown-linux-gnu\n            architecture: x86_64",
        "continue-on-error: ${{ matrix.continue_on_error }}",
        "continue_on_error: false",
        "continue_on_error: true",
        "test \"$(uname -m)\" = \"${{ matrix.architecture }}\"",
        "node --test scripts/check-desktop-transport.cjs",
        "validation-only-linux-x86_64-unqualified",
    ] {
        assert!(workflow.contains(required), "CI is missing {required:?}");
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

/// Ensures tagged releases aggregate one verified macOS archive and one source archive.
#[test]
fn release_publishes_checksummed_native_assets() {
    let workflow = repository_file(".github/workflows/release.yml");
    for required in [
        "git cat-file -t",
        "Cargo.toml",
        "cargo xtask check",
        "cargo xtask release build-native",
        "cargo xtask release verify",
        "cargo xtask archive --revision HEAD",
        "runs-on: macos-15",
        "--target aarch64-apple-darwin",
        "agent-run-$version-aarch64-apple-darwin.tar.gz",
        "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
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
    for forbidden in ["x86_64-unknown-linux-gnu", "matrix.target"] {
        assert!(
            !workflow.contains(forbidden),
            "release contains an unqualified target {forbidden:?}"
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
        "the macOS native job must run the full locked workspace gates"
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
